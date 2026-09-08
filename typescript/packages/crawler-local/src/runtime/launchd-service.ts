import { hash } from "node:crypto";
import { constants } from "node:fs";
import { access, lstat, readFile, stat, statfs } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { z } from "zod";

import {
  LaunchdServiceLabelSchema,
  minimumLaunchdExitTimeoutSeconds,
} from "./launchd-contract.js";
import { loadScraperOperationConfig } from "./operations-contract.js";
import { readRunLock } from "./run-lock.js";
import { RUNTIME_ENVIRONMENT } from "./runtime-environment.js";

const AbsolutePathSchema = z
  .string()
  .min(1)
  .max(4_096)
  .refine(isAbsolute, "launchd paths must be absolute");
const SERVICE_ENABLED_SENTINEL = "SERVICE_ENABLED";
const DEFAULT_HOME = homedir();
const DEFAULT_EXECUTABLE_SEARCH_PATH = [
  "/opt/homebrew/bin",
  join(DEFAULT_HOME, ".local", "bin"),
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
].join(":");

export const LaunchdServiceOptionsSchema = z
  .object({
    codexHomePath: AbsolutePathSchema.default(() =>
      resolve(environmentCodexHome() ?? join(DEFAULT_HOME, ".codex")),
    ),
    configPath: AbsolutePathSchema,
    executablePath: AbsolutePathSchema,
    executableSearchPath: z
      .string()
      .min(1)
      .max(16_384)
      .refine((value) => value.split(":").every(isAbsolute))
      .default(DEFAULT_EXECUTABLE_SEARCH_PATH),
    exitTimeOutSeconds: z.int().min(30).max(7_200).default(135),
    label: LaunchdServiceLabelSchema,
    homePath: AbsolutePathSchema.default(DEFAULT_HOME),
    standardErrorPath: AbsolutePathSchema,
    standardOutPath: AbsolutePathSchema,
    throttleIntervalSeconds: z.int().min(30).max(3_600).default(60),
    workingDirectory: AbsolutePathSchema,
  })
  .strict();

export type LaunchdServiceOptions = z.infer<typeof LaunchdServiceOptionsSchema>;

export interface LaunchdPreflightIssue {
  readonly code: string;
  readonly message: string;
}

export interface LaunchdPreflightReport {
  readonly availableBytes: null | number;
  readonly codexAuth: LaunchdCodexAuthDiagnostic;
  readonly configDigest: null | string;
  readonly issues: readonly LaunchdPreflightIssue[];
  readonly ok: boolean;
  readonly options: LaunchdServiceOptions;
  readonly plist: string;
}

export interface LaunchdCodexAuthDiagnostic {
  readonly authPathHash: string;
  readonly homePathHash: string;
  readonly resolution: "default" | "environment" | "explicit";
  readonly state: "missing" | "readable" | "unreadable";
}

export async function renderLaunchdService(
  input: unknown,
  stateDirectory?: string,
): Promise<string> {
  const options = LaunchdServiceOptionsSchema.parse(input);
  if (!stateDirectory) await loadScraperOperationConfig(options.configPath);
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "https://www.apple.com/DTDs/PropertyList-1.0.dtd">',
    '<plist version="1.0">',
    "<dict>",
    "  <key>Label</key>",
    `  ${plistValue(options.label)}`,
    "  <key>ProgramArguments</key>",
    "  <array>",
    `    ${plistValue(options.executablePath)}`,
    `    ${plistValue("run-service")}`,
    `    ${plistValue("--config")}`,
    `    ${plistValue(options.configPath)}`,
    "  </array>",
    "  <key>EnvironmentVariables</key>",
    "  <dict>",
    `    ${plistKey("CODEX_HOME")}`,
    `    ${plistValue(options.codexHomePath)}`,
    `    ${plistKey("HOME")}`,
    `    ${plistValue(options.homePath)}`,
    `    ${plistKey("LANG")}`,
    `    ${plistValue("en_US.UTF-8")}`,
    `    ${plistKey("PATH")}`,
    `    ${plistValue(options.executableSearchPath)}`,
    "  </dict>",
    "  <key>WorkingDirectory</key>",
    `  ${plistValue(options.workingDirectory)}`,
    "  <key>StandardOutPath</key>",
    `  ${plistValue(options.standardOutPath)}`,
    "  <key>StandardErrorPath</key>",
    `  ${plistValue(options.standardErrorPath)}`,
    "  <key>RunAtLoad</key>",
    "  <true/>",
    "  <key>KeepAlive</key>",
    "  <dict>",
    "    <key>SuccessfulExit</key>",
    "    <false/>",
    "  </dict>",
    "  <key>ThrottleInterval</key>",
    `  <integer>${String(options.throttleIntervalSeconds)}</integer>`,
    "  <key>ProcessType</key>",
    `  ${plistValue("Background")}`,
    "  <key>LowPriorityIO</key>",
    "  <true/>",
    "  <key>SoftResourceLimits</key>",
    "  <dict>",
    "    <key>NumberOfFiles</key>",
    "    <integer>4096</integer>",
    "  </dict>",
    "  <key>HardResourceLimits</key>",
    "  <dict>",
    "    <key>NumberOfFiles</key>",
    "    <integer>4096</integer>",
    "  </dict>",
    "  <key>AbandonProcessGroup</key>",
    "  <false/>",
    "  <key>ExitTimeOut</key>",
    `  <integer>${String(options.exitTimeOutSeconds)}</integer>`,
    "  <key>Umask</key>",
    "  <integer>63</integer>",
    "</dict>",
    "</plist>",
    "",
  ].join("\n");
}

export function serviceEnabledPath(stateDirectory: string): string {
  return resolve(stateDirectory, SERVICE_ENABLED_SENTINEL);
}

function plistValue(text: string): string {
  return `<string>${escapeXml(text)}</string>`;
}

function plistKey(text: string): string {
  return `<key>${escapeXml(text)}</key>`;
}

/** Read-only validation. It never creates state, writes a plist, or invokes launchctl. */
export async function preflightLaunchdService(
  input: unknown,
): Promise<LaunchdPreflightReport> {
  const codexHomeResolution = explicitCodexHomePath(input)
    ? "explicit"
    : environmentCodexHome() === null
      ? "default"
      : "environment";
  const options = LaunchdServiceOptionsSchema.parse(input);
  const issues: LaunchdPreflightIssue[] = [];
  let configDigest: null | string = null;
  let availableBytes: null | number = null;
  let minimumFreeBytes = 0;
  let stateDirectory = dirname(options.configPath);
  try {
    const loaded = await loadScraperOperationConfig(options.configPath);
    stateDirectory = loaded.config.stateDirectory;
    configDigest = loaded.configDigest;
    minimumFreeBytes = loaded.config.retention.minimumFreeBytes;
    if (
      options.exitTimeOutSeconds <
      minimumLaunchdExitTimeoutSeconds(loaded.config.restart.shutdownGraceMs)
    )
      issues.push(
        issue(
          "EXIT_TIMEOUT_TOO_SHORT",
          "launchd ExitTimeOut must cover supervisor shutdown grace plus child cleanup",
        ),
      );
    if (!(await pathExists(loaded.config.stateDirectory))) {
      issues.push(
        issue("STATE_MISSING", "Configured state directory does not exist"),
      );
    } else {
      const filesystem = await statfs(loaded.config.stateDirectory);
      availableBytes = filesystem.bavail * filesystem.bsize;
      if (
        !Number.isSafeInteger(availableBytes) ||
        availableBytes < minimumFreeBytes
      )
        issues.push(
          issue("DISK_PRESSURE", "Free disk is below the configured reserve"),
        );
      const lockPath = resolve(loaded.config.stateDirectory, "RUN.lock");
      const lock = await readRunLock(lockPath);
      if (lock !== null) {
        issues.push(
          issue(
            "RUN_LOCK_PRESENT",
            `Runtime owner is present for pid ${String(lock.pid)}; refuse overlapping service`,
          ),
        );
      }
    }
  } catch (error) {
    issues.push(
      issue(
        "CONFIG_INVALID",
        error instanceof Error ? error.message : "Configuration is invalid",
      ),
    );
  }
  await requireExecutable(options.executablePath, issues);
  await requireDirectory(options.workingDirectory, "WORKDIR_INVALID", issues);
  await Promise.all(
    [options.standardOutPath, options.standardErrorPath].map((path) =>
      requireDirectory(dirname(path), "LOG_DIRECTORY_INVALID", issues),
    ),
  );
  const codexAuth = await inspectCodexAuth(
    options.codexHomePath,
    codexHomeResolution,
  );
  return {
    availableBytes,
    codexAuth,
    configDigest,
    issues,
    ok: issues.length === 0,
    options,
    plist: await renderLaunchdService(options, stateDirectory),
  };
}

async function inspectCodexAuth(
  codexHomePath: string,
  resolution: LaunchdCodexAuthDiagnostic["resolution"],
): Promise<LaunchdCodexAuthDiagnostic> {
  const authPath = join(codexHomePath, "auth.json");
  let state: LaunchdCodexAuthDiagnostic["state"] = "readable";
  try {
    const information = await stat(authPath);
    if (!information.isFile()) state = "unreadable";
    else await access(authPath, constants.R_OK);
  } catch (error) {
    state = errorCode(error) === "ENOENT" ? "missing" : "unreadable";
  }
  return {
    authPathHash: hash("sha256", authPath, "hex"),
    homePathHash: hash("sha256", codexHomePath, "hex"),
    resolution,
    state,
  };
}

function explicitCodexHomePath(input: unknown): boolean {
  return (
    typeof input === "object" &&
    input !== null &&
    Object.hasOwn(input, "codexHomePath")
  );
}

function environmentCodexHome(): null | string {
  const value = RUNTIME_ENVIRONMENT["CODEX_HOME"];
  return value === undefined || value.length === 0 ? null : value;
}

async function requireExecutable(
  path: string,
  issues: LaunchdPreflightIssue[],
): Promise<void> {
  try {
    const information = await lstat(path);
    if (!information.isFile()) throw new Error("not a regular file");
    await access(path, constants.R_OK | constants.X_OK);
    const contents = await readFile(path, "utf8");
    const firstLine = contents.split("\n", 1)[0];
    if (!firstLine?.startsWith("#!"))
      throw new Error("missing executable shebang");
  } catch (error) {
    issues.push(
      issue(
        "CLI_NOT_EXECUTABLE",
        `Built crawler CLI is not executable: ${error instanceof Error ? error.message : String(error)}`,
      ),
    );
  }
}

async function requireDirectory(
  path: string,
  code: string,
  issues: LaunchdPreflightIssue[],
): Promise<void> {
  try {
    const information = await lstat(path);
    if (!information.isDirectory()) throw new Error("not a directory");
    await access(path, constants.R_OK | constants.W_OK | constants.X_OK);
  } catch (error) {
    issues.push(
      issue(
        code,
        `${path}: ${error instanceof Error ? error.message : String(error)}`,
      ),
    );
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch (error) {
    if (errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

function errorCode(error: unknown): null | string {
  if (typeof error !== "object" || error === null || !("code" in error))
    return null;
  return typeof error.code === "string" ? error.code : null;
}

function issue(code: string, message: string): LaunchdPreflightIssue {
  return { code, message };
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}
