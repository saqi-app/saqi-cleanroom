import { randomUUID } from "node:crypto";
import {
  appendFile,
  mkdir,
  readdir,
  rename,
  stat,
  unlink,
} from "node:fs/promises";
import { resolve } from "node:path";

export interface SupervisorLogEvent {
  readonly attempt?: number;
  readonly durationMs?: number;
  readonly errorCode?: string;
  readonly event: string;
  readonly lane: string;
  readonly result?: string;
  readonly runId: string;
  readonly timestamp: string;
  readonly workKey?: string;
}

export interface JsonlLoggerHealth {
  readonly droppedEvents: number;
  readonly lastErrorCode: null | string;
  readonly retryAt: null | number;
  readonly state: "pressure_wait" | "ready";
}

export interface JsonlLoggerOptions {
  readonly append?: (path: string, value: string) => Promise<void> | void;
  readonly now?: () => number;
}

const LOG_PRESSURE_RETRY_MS = 30_000;

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete log-file owner serializes rotation and shutdown; filesystem append behavior is already injectable.
export class RotatingJsonlLogger {
  readonly #directory: string;
  readonly #append: (path: string, value: string) => Promise<void> | void;
  readonly #maximumBytes: number;
  readonly #now: () => number;
  readonly #path: string;
  readonly #retainedFiles: number;
  #initialized = false;
  #tail: Promise<void> = Promise.resolve();
  #droppedEvents = 0;
  #lastErrorCode: null | string = null;
  #retryAt: null | number = null;

  constructor(
    directory: string,
    maximumBytes: number,
    retainedFiles: number,
    options: JsonlLoggerOptions = {},
  ) {
    this.#directory = directory;
    this.#append =
      options.append ??
      ((path, value) =>
        appendFile(path, value, { encoding: "utf8", mode: 0o600 }));
    this.#maximumBytes = maximumBytes;
    this.#now = options.now ?? Date.now;
    this.#retainedFiles = retainedFiles;
    this.#path = resolve(directory, "events.jsonl");
  }

  get path(): string {
    return this.#path;
  }

  health(): JsonlLoggerHealth {
    return {
      droppedEvents: this.#droppedEvents,
      lastErrorCode: this.#lastErrorCode,
      retryAt: this.#retryAt,
      state: this.#retryAt === null ? "ready" : "pressure_wait",
    };
  }

  async write(event: SupervisorLogEvent): Promise<void> {
    const pending = this.#tail.then(() => this.#write(event));
    this.#tail = pending.catch((error: unknown) => {
      console.error(
        "SAQI_JSONL_LOG_WRITE_FAILED",
        filesystemErrorCode(error) ?? "UNKNOWN",
      );
    });
    await pending;
  }

  async #write(event: SupervisorLogEvent): Promise<void> {
    if (!this.#initialized) {
      await mkdir(this.#directory, { mode: 0o700, recursive: true });
      await this.#prune();
      this.#initialized = true;
    }
    const line = `${JSON.stringify(event)}\n`;
    const bytes = Buffer.byteLength(line);
    if (bytes > this.#maximumBytes)
      throw new Error("Structured log event exceeds maximum log size");
    const now = this.#now();
    if (this.#retryAt !== null && now < this.#retryAt) {
      this.#droppedEvents += 1;
      return;
    }
    try {
      let currentBytes = 0;
      try {
        const status = await stat(this.#path);
        currentBytes = status.size;
      } catch (error) {
        if (filesystemErrorCode(error) !== "ENOENT") throw error;
      }
      if (currentBytes + bytes > this.#maximumBytes && currentBytes > 0) {
        const archive = resolve(
          this.#directory,
          `events.${now.toString().padStart(13, "0")}.${randomUUID()}.jsonl`,
        );
        await rename(this.#path, archive);
        await this.#prune();
      }
      await this.#append(this.#path, line);
      this.#lastErrorCode = null;
      this.#retryAt = null;
    } catch (error) {
      const code = filesystemErrorCode(error);
      if (
        !code ||
        !["EDQUOT", "EIO", "EMFILE", "ENFILE", "ENOSPC", "EROFS"].includes(code)
      )
        throw error;
      this.#droppedEvents += 1;
      this.#lastErrorCode = code;
      this.#retryAt = now + LOG_PRESSURE_RETRY_MS;
    }
  }

  async #prune(): Promise<void> {
    const directoryEntries = await readdir(this.#directory);
    const archives = directoryEntries
      .filter((name) => /^events\.\d{13}\.[\da-f-]{36}\.jsonl$/.test(name))
      .toSorted();
    await Promise.all(
      archives
        .slice(0, -this.#retainedFiles)
        .map((name) => unlink(resolve(this.#directory, name))),
    );
  }
}

function filesystemErrorCode(error: unknown): null | string {
  if (typeof error !== "object" || error === null || !("code" in error))
    return null;
  return typeof error.code === "string" ? error.code : null;
}
