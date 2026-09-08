#!/usr/bin/env node

import { access, lstat, mkdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { PoemEnrichmentInputSchema } from "@saqi/precedent-iso";
import { configureSource, currentSource } from "@saqi/source-adapter";
import { z } from "zod";

import {
  seedAuthorManifest,
  seedAuthorManifests,
} from "./collection/collector.js";
import { CollectorRecoveryController } from "./collection/collector-recovery.js";
import { parseCatalogInventory } from "./collection/inventory-reconciliation.js";
import { SolEnrichmentCoordinator } from "./enrichment/sol-coordinator.js";
import {
  CodexSolRunner,
  type EnrichmentProvider,
} from "./enrichment/sol-runner.js";
import {
  ArtifactStore,
  verifyArtifactInventory,
} from "./persistence/artifact-store.js";
import { exportProductionBaseline } from "./persistence/baseline-export.js";
import { Ledger } from "./persistence/ledger.js";
import { PauseControls } from "./persistence/pause-controls.js";
import {
  seedProductionDetailRecoveryFiles,
  verifyProductionDetailRecoveryFiles,
} from "./persistence/production-baseline-planner.js";
import { exportProductionResolution } from "./persistence/production-resolution-store.js";
import {
  fetchScopedProductionResolution,
  ScopedProductionResolutionStore,
} from "./persistence/scoped-production-resolution.js";
import { importLegacySolOperations } from "./persistence/sol-operation-import.js";
import { canonicalJson, sha256 } from "./persistence/work-key.js";
import { prepareCollectedPoem } from "./publication/corpus-import-actions.js";
import { loadCliSourceConfiguration } from "./runtime/cli-source-policy.js";
import {
  AllowedConcurrencySchema,
  applyRigConcurrency,
  ConcurrencyProviderSchema,
} from "./runtime/concurrency-control.js";
import { ConcurrencyStore } from "./runtime/concurrency-store.js";
import { controlLaunchdService } from "./runtime/launchd-control.js";
import { preflightLaunchdService } from "./runtime/launchd-service.js";
import { loadScraperOperationConfig } from "./runtime/operations-contract.js";
import {
  evaluatePipelineHealth,
  readPipelineHealth,
  readSolQuotaSignal,
} from "./runtime/pipeline-diagnostics.js";
import { ResourcePressureMonitor } from "./runtime/resource-pressure.js";
import { acquireRunLock, inspectRunOwner } from "./runtime/run-lock.js";
import { RUNTIME_ENVIRONMENT } from "./runtime/runtime-environment.js";
import {
  installRuntime,
  readRuntimeReleaseIdentity,
  retainRuntimeReleases,
} from "./runtime/runtime-installer.js";
import { readServiceEnabled } from "./runtime/service-enabled-control.js";
import { inspectServiceStatus } from "./runtime/service-status-inspection.js";
import {
  loadLaunchdDesiredConfigDigest,
  loadLaunchdSourceConfiguration,
} from "./runtime/source-keychain.js";
import { inspectStateInventory } from "./runtime/state-inventory.js";
import { UnifiedSupervisor } from "./runtime/supervisor.js";
import { UnifiedRigRuntime } from "./runtime/unified-rig.js";

interface Locations {
  readonly artifacts: string;
  readonly database: string;
  readonly paidWorkPaused: string;
  readonly paused: string;
  readonly root: string;
  readonly schedulerState: string;
  readonly solAttempts: string;
}

const CONFIG_RELOAD_INTERVAL_MS = 60_000;
const HELP = `Usage: saqi-crawler <command> [options]

Long-running rig:
  run --config FILE [--max-cycles N] [--maximum-runtime-ms N]
  service-control --action <status|start|stop|restart> --config FILE [--label LABEL]
  set-concurrency --config FILE --provider sol --value <2|4|8|16|32|128|256> [--initial-value <2|4|8|16|32|128|256>] [--label LABEL]
  install-service --dry-run --config FILE --executable PATH --workdir PATH --stdout PATH --stderr PATH [--codex-home DIR]
  install-runtime --repository DIR --release-root DIR --commit SHA
  runtime-retention --repository DIR --release-root DIR [--retain-rollbacks N] [--apply --maximum-deletions N]

Inspect and recover:
  import-sol-operations [--config FILE | --state-dir DIR] [--dry-run | --apply --expected-digest SHA256]
  status [--config FILE | --state-dir DIR]
  health [--config FILE | --state-dir DIR] [--format json] [--fail-on-blocked]
  doctor [--config FILE | --state-dir DIR] [--deep-integrity]
  verify [--config FILE | --state-dir DIR]
  pause | resume | pause-paid | resume-paid --maximum-sol-operations N [--rearm] | clear-source-stop --confirm
  collector-recovery --action <status|arm> [--config FILE | --state-dir DIR]

One-shot maintenance and enrichment:
  init [--state-dir DIR]
  seed-author --author URL
  seed-authors --input FILE [--refresh-generation ID]
  seed-enrichment --input FILE [--provider sol] [--priority N]
  run-enrichment [--provider sol] [--max N]
  prepare-import --artifact FILE --mapping FILE --observed-at ISO --writer-epoch N
  export-baseline --database FILE --authors-output FILE --poems-output FILE
  recover-legacy-sources --manifest FILE --authors FILE --poems FILE [--state-dir DIR] [--dry-run | --apply]
  export-resolution --database FILE --output FILE --observed-at ISO
  fetch-resolution --config FILE --request FILE --output FILE [--endpoint URL]
  validate-resolution --input FILE

Unless --config is supplied, stateful commands use --state-dir DIR or .saqi-runs.
All machine-readable command results are JSON.`;

const CatalogPoemMappingSchema = z.strictObject({
  authorId: z.string().min(1),
  authorNameArabic: z.string().min(1),
  poemId: z.string().min(1),
  sourceAuthorSlug: z.string().min(1),
  sourcePoemId: z.string().regex(/^[1-9]\d*$/),
});
const MaximumReleaseDeletionsSchema = z.coerce.number().int().positive();
const RetainRollbackCountSchema = z.coerce.number().int().min(3);
const EnrichmentProviderSchema = z.literal("sol");
const CollectorRecoveryActionSchema = z.enum(["arm", "status"]);

async function main(commandArguments: readonly string[]): Promise<void> {
  const invokedCommand = commandArguments[0];
  const serviceMode = invokedCommand === "run-service";
  const command = serviceMode ? "run" : invokedCommand;
  if (command === "run-collector")
    throw new Error(
      "run-collector was removed; use `saqi-crawler run --config FILE` so collection obeys RUN.lock and collector.enabled",
    );
  if (command === "requeue-render-failures")
    throw new Error(
      "requeue-render-failures was removed; enable collector.recovery and use `saqi-crawler collector-recovery --action arm --config FILE` for bounded recovery",
    );
  if (command === "--help" || command === "-h" || command === "help") {
    process.stdout.write(`${HELP}\n`);
    return;
  }
  if (serviceMode) {
    const configPath = option(commandArguments, "--config");
    if (!configPath) throw new Error("run-service requires --config");
    const loaded = await loadScraperOperationConfig(configPath);
    if (!readServiceEnabled(loaded.config.stateDirectory)) {
      print({
        command: invokedCommand,
        serviceEnabled: false,
        result: { stopped: "disabled", cycles: 0 },
      });
      return;
    }
  }
  if (
    command === "service-control" &&
    option(commandArguments, "--action") === "status"
  ) {
    const configPath = option(commandArguments, "--config");
    if (!configPath)
      throw new Error("service-control requires --config and --action");
    print({
      command,
      result: await inspectServiceStatus(
        configPath,
        option(commandArguments, "--label") ?? undefined,
        loadLaunchdSourceConfiguration,
      ),
    });
    return;
  }
  const source = await loadCliSourceConfiguration({
    command: invokedCommand,
    commandArguments,
    environment: RUNTIME_ENVIRONMENT,
    loadManagedSource: loadLaunchdSourceConfiguration,
  });
  if (source !== null) configureSource(source);
  if (command === "service-control") {
    const configPath = option(commandArguments, "--config");
    const action = option(commandArguments, "--action");
    const label = option(commandArguments, "--label");
    if (!configPath || !action)
      throw new Error("service-control requires --config and --action");
    print({
      command,
      result: await controlLaunchdService({
        action,
        configPath,
        ...(label === null ? {} : { label }),
      }),
    });
    return;
  }
  if (command === "set-concurrency") {
    const configPath = option(commandArguments, "--config");
    const rawProvider = option(commandArguments, "--provider");
    const rawValue = option(commandArguments, "--value");
    const rawInitialValue = option(commandArguments, "--initial-value");
    const label = option(commandArguments, "--label");
    if (!configPath || !rawProvider || !rawValue) {
      throw new Error(
        "set-concurrency requires --config, --provider, and --value",
      );
    }
    const provider = ConcurrencyProviderSchema.parse(rawProvider);
    const value = AllowedConcurrencySchema.parse(Number(rawValue));
    const initialValue =
      rawInitialValue === null
        ? value
        : AllowedConcurrencySchema.parse(Number(rawInitialValue));
    print({
      command,
      result: await applyRigConcurrency(
        configPath,
        provider,
        value,
        label ?? undefined,
        undefined,
        initialValue,
      ),
    });
    return;
  }
  if (command === "install-runtime") {
    const repository = option(commandArguments, "--repository");
    const releaseRoot = option(commandArguments, "--release-root");
    const commit = option(commandArguments, "--commit");
    if (!repository || !releaseRoot || !commit) {
      throw new Error(
        "install-runtime requires --repository, --release-root, and --commit",
      );
    }
    print({
      command,
      result: await installRuntime({ commit, releaseRoot, repository }),
    });
    return;
  }
  if (command === "runtime-retention") {
    const repository = option(commandArguments, "--repository");
    const releaseRoot = option(commandArguments, "--release-root");
    const retainRollbacks = option(commandArguments, "--retain-rollbacks");
    const maximumDeletions = option(commandArguments, "--maximum-deletions");
    if (!repository || !releaseRoot) {
      throw new Error(
        "runtime-retention requires --repository and --release-root",
      );
    }
    print({
      command,
      result: await retainRuntimeReleases({
        apply: commandArguments.includes("--apply"),
        releaseRoot,
        repository,
        ...(maximumDeletions === null
          ? {}
          : {
              maximumDeletions:
                MaximumReleaseDeletionsSchema.parse(maximumDeletions),
            }),
        ...(retainRollbacks === null
          ? {}
          : {
              retainRollbackCount:
                RetainRollbackCountSchema.parse(retainRollbacks),
            }),
      }),
    });
    return;
  }
  if (command === "export-baseline") {
    const database = option(commandArguments, "--database");
    const authorsOutput = option(commandArguments, "--authors-output");
    const poemsOutput = option(commandArguments, "--poems-output");
    if (!database || !authorsOutput || !poemsOutput) {
      throw new Error(
        "export-baseline requires --database, --authors-output, and --poems-output",
      );
    }
    print({
      command,
      result: await exportProductionBaseline({
        authorsOutput,
        database,
        poemsOutput,
      }),
    });
    return;
  }
  if (command === "export-resolution") {
    const database = option(commandArguments, "--database");
    const output = option(commandArguments, "--output");
    const observedAt = option(commandArguments, "--observed-at");
    if (!database || !output || !observedAt) {
      throw new Error(
        "export-resolution requires --database, --output, and --observed-at",
      );
    }
    print({
      command,
      result: await exportProductionResolution({
        database,
        observedAt,
        output,
      }),
    });
    return;
  }
  if (command === "fetch-resolution") {
    const configPath = option(commandArguments, "--config");
    const requestPath = option(commandArguments, "--request");
    const output = option(commandArguments, "--output");
    const explicitEndpoint = option(commandArguments, "--endpoint");
    if (!configPath || !requestPath || !output) {
      throw new Error(
        "fetch-resolution requires --config, --request, and --output",
      );
    }
    const { config } = await loadScraperOperationConfig(configPath);
    const publicationEndpoint = config.publication.endpoint;
    const endpoint =
      explicitEndpoint ??
      (publicationEndpoint
        ? new URL("/api/corpus-resolution", publicationEndpoint).toString()
        : null);
    if (!endpoint) {
      throw new Error(
        "fetch-resolution requires --endpoint or a configured publication endpoint",
      );
    }
    print({
      command,
      result: await fetchScopedProductionResolution({
        auth: config.publication.auth,
        endpoint,
        output,
        request: await readJson(resolve(requestPath)),
      }),
    });
    return;
  }
  if (command === "validate-resolution") {
    const input = option(commandArguments, "--input");
    if (!input) throw new Error("validate-resolution requires --input");
    const store = await ScopedProductionResolutionStore.open(input);
    try {
      print({ command, result: await store.report() });
    } finally {
      store.close();
    }
    return;
  }
  if (command === "run") {
    const configPath = option(commandArguments, "--config");
    if (!configPath) throw new Error("run requires --config FILE");
    const maximumCyclesRaw = option(commandArguments, "--max-cycles");
    const maximumCycles =
      maximumCyclesRaw === null ? undefined : Number(maximumCyclesRaw);
    const maximumRuntimeRaw = option(commandArguments, "--maximum-runtime-ms");
    const maximumRuntimeMs =
      maximumRuntimeRaw === null ? undefined : Number(maximumRuntimeRaw);
    if (
      maximumCycles !== undefined &&
      (!Number.isSafeInteger(maximumCycles) || maximumCycles < 1)
    ) {
      throw new Error("--max-cycles must be a positive integer");
    }
    if (
      maximumRuntimeMs !== undefined &&
      (!Number.isSafeInteger(maximumRuntimeMs) || maximumRuntimeMs < 1_000)
    ) {
      throw new Error(
        "--maximum-runtime-ms must be an integer of at least 1000",
      );
    }
    const bootstrap = await loadScraperOperationConfig(configPath);
    // Startup owns initialization. Read-only status/config inspection never
    // creates a ledger or consumes the legacy concurrency configuration.
    const bootstrapLedger = Ledger.initialize(
      resolve(bootstrap.config.stateDirectory, "ledger.sqlite3"),
    );
    bootstrapLedger.close();
    ConcurrencyStore.withDatabase(
      bootstrap.config.stateDirectory,
      false,
      (store) => store.initialize(bootstrap.config.sol),
    );
    const loaded = await loadScraperOperationConfig(configPath);
    const configured = loaded.config;
    const paths = locations(["--state-dir", configured.stateDirectory]);
    // Credential bootstrap may overlap an operator stop. Recheck intent
    // before scheduler admission or construction of any runtime lanes.
    if (serviceMode && !readServiceEnabled(paths.root)) {
      print({
        command: invokedCommand,
        serviceEnabled: false,
        result: { stopped: "disabled", cycles: 0 },
      });
      return;
    }
    let requireCurrentSchedulerAuthority = false;
    if (serviceMode && configured.sol.enabled) {
      const authorityLedger = await openExisting(paths.database);
      try {
        const stateInventory = await inspectStateInventory({
          ledger: authorityLedger,
          root: paths.root,
        });
        if (!stateInventory.codexScheduler.safeToOperate) {
          throw new Error("SOL_SCHEDULER_CURRENT_AUTHORITY_REQUIRED");
        }
        requireCurrentSchedulerAuthority = true;
      } finally {
        authorityLedger.close();
      }
    }
    const controller = new AbortController();
    const drainController = new AbortController();
    let terminationSignals = 0;
    let signalGraceTimer: NodeJS.Timeout | undefined;
    const abort = () => {
      terminationSignals += 1;
      if (terminationSignals === 1) {
        drainController.abort(new Error("Operator drain requested"));
        signalGraceTimer = setTimeout(
          () => controller.abort(new Error("Operator drain grace exceeded")),
          configured.restart.shutdownGraceMs,
        );
        signalGraceTimer.unref();
      } else {
        controller.abort(new Error("Operator hard stop"));
      }
    };
    process.on("SIGINT", abort);
    process.on("SIGTERM", abort);
    let desiredConfigDigest: null | string = loaded.configDigest;
    let nextConfigReloadAt = 0;
    const readDesiredConfigDigest = async (): Promise<null | string> => {
      const now = Date.now();
      if (now < nextConfigReloadAt) return desiredConfigDigest;
      nextConfigReloadAt = now + CONFIG_RELOAD_INTERVAL_MS;
      try {
        if (serviceMode) {
          desiredConfigDigest =
            await loadLaunchdDesiredConfigDigest(configPath);
        } else {
          const reloaded = await loadScraperOperationConfig(configPath);
          desiredConfigDigest = reloaded.configDigest;
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error("SAQI_CONFIG_RELOAD_FAILED", message);
        desiredConfigDigest = null;
      }
      return desiredConfigDigest;
    };
    const resourcePressure = new ResourcePressureMonitor(
      configured.stateDirectory,
      configured.resources,
    );
    const runtime = new UnifiedRigRuntime({
      config: configured,
      configDigest: loaded.configDigest,
      desiredConfigDigest: readDesiredConfigDigest,
      paths,
      requireCurrentSchedulerAuthority,
      resourcePressure: () => resourcePressure.snapshot(),
    });
    const supervisor = new UnifiedSupervisor({
      config: configured,
      configDigest: loaded.configDigest,
      createLanes: (preflight) => runtime.createLanes(preflight),
      desiredConfigDigest: readDesiredConfigDigest,
      paused: () => runtime.paused(),
      preflight: () => runtime.preflight(),
      resourcePressure,
      runtimeRelease: await readRuntimeReleaseIdentity(process.argv[1] ?? ""),
      providerStatus: () => runtime.providerExecutionStatus(),
      status: () => runtime.status(),
    });
    try {
      print({
        command,
        configDigest: loaded.configDigest,
        configPath: loaded.configPath,
        result: await supervisor.run(controller.signal, {
          drainSignal: drainController.signal,
          ...(maximumCycles === undefined ? {} : { maximumCycles }),
          ...(maximumRuntimeMs === undefined ? {} : { maximumRuntimeMs }),
        }),
      });
    } finally {
      process.off("SIGINT", abort);
      process.off("SIGTERM", abort);
      if (signalGraceTimer !== undefined) clearTimeout(signalGraceTimer);
      runtime.close();
    }
    if (serviceMode && readServiceEnabled(paths.root)) process.exitCode = 1;
    return;
  }
  if (command === "install-service") {
    if (!commandArguments.includes("--dry-run"))
      throw new Error("install-service supports only --dry-run");
    const configPath = option(commandArguments, "--config");
    const executablePath = option(commandArguments, "--executable");
    const workingDirectory = option(commandArguments, "--workdir");
    const standardOutPath = option(commandArguments, "--stdout");
    const standardErrorPath = option(commandArguments, "--stderr");
    const codexHomePath = option(commandArguments, "--codex-home");
    if (
      !configPath ||
      !executablePath ||
      !workingDirectory ||
      !standardOutPath ||
      !standardErrorPath
    ) {
      throw new Error(
        "install-service --dry-run requires --config, --executable, --workdir, --stdout, and --stderr",
      );
    }
    const label = option(commandArguments, "--label");
    const throttle = option(commandArguments, "--throttle");
    const exitTimeout = option(commandArguments, "--exit-timeout");
    const report = await preflightLaunchdService({
      ...(codexHomePath === null
        ? {}
        : { codexHomePath: resolve(codexHomePath) }),
      configPath: resolve(configPath),
      executablePath: resolve(executablePath),
      ...(exitTimeout === null
        ? {}
        : { exitTimeOutSeconds: Number(exitTimeout) }),
      ...(label === null ? {} : { label }),
      standardErrorPath: resolve(standardErrorPath),
      standardOutPath: resolve(standardOutPath),
      ...(throttle === null
        ? {}
        : { throttleIntervalSeconds: Number(throttle) }),
      workingDirectory: resolve(workingDirectory),
    });
    print({ command, dryRun: true, report });
    if (!report.ok) process.exitCode = 2;
    return;
  }
  const sharedConfigPath = option(commandArguments, "--config");
  if (
    sharedConfigPath !== null &&
    option(commandArguments, "--state-dir") !== null
  ) {
    throw new Error("--config and --state-dir are mutually exclusive");
  }
  const sharedConfiguration =
    sharedConfigPath === null
      ? null
      : await loadScraperOperationConfig(sharedConfigPath);
  const paths = locations(
    sharedConfiguration === null
      ? commandArguments
      : ["--state-dir", sharedConfiguration.config.stateDirectory],
  );
  if (command === "import-sol-operations") {
    const apply = commandArguments.includes("--apply");
    const expectedDigest = option(commandArguments, "--expected-digest");
    if (apply && commandArguments.includes("--dry-run"))
      throw new Error("--apply and --dry-run are mutually exclusive");
    if (apply !== (expectedDigest !== null))
      throw new Error(
        "--apply requires --expected-digest from a prior dry-run; --expected-digest requires --apply",
      );
    const result = await importLegacySolOperations(
      apply && expectedDigest !== null
        ? { stateDirectory: paths.root, apply: true, expectedDigest }
        : { stateDirectory: paths.root },
    );
    print({ command, result });
    return;
  }
  if (command === "collector-recovery") {
    const action = CollectorRecoveryActionSchema.parse(
      option(commandArguments, "--action"),
    );
    if (action === "arm") {
      if (!sharedConfiguration)
        throw new Error("collector-recovery arm requires --config FILE");
      if (!sharedConfiguration.config.collector.recovery.enabled)
        throw new Error("COLLECTOR_RECOVERY_NOT_ENABLED");
    }
    const ledger = await openExisting(paths.database, {
      readonly: action === "status",
    });
    try {
      const recovery = new CollectorRecoveryController({ ledger });
      print({
        action,
        command,
        state: action === "arm" ? recovery.arm() : recovery.status(),
      });
    } finally {
      ledger.close();
    }
    return;
  }
  if (command === "recover-legacy-sources") {
    const manifest = option(commandArguments, "--manifest");
    const authorsPath = option(commandArguments, "--authors");
    const poemsPath = option(commandArguments, "--poems");
    const apply = commandArguments.includes("--apply");
    if (apply && commandArguments.includes("--dry-run")) {
      throw new Error("recover-legacy-sources accepts only one execution mode");
    }
    if (!manifest || !authorsPath || !poemsPath) {
      throw new Error(
        "recover-legacy-sources requires --manifest, --authors, and --poems",
      );
    }
    const resolvedAuthorsPath = resolve(authorsPath);
    const resolvedPoemsPath = resolve(poemsPath);
    const parsedManifest = await readJson(resolve(manifest));
    await verifyProductionDetailRecoveryFiles({
      authorsPath: resolvedAuthorsPath,
      manifest: parsedManifest,
      poemsPath: resolvedPoemsPath,
    });
    const ledger = await openExisting(paths.database, { readonly: !apply });
    try {
      if (apply && !ledger.pauseControls.read().paidWorkPaused)
        throw new Error("RECOVERY_REQUIRES_PAID_WORK_PAUSED");
      print({
        command,
        mode: apply ? "apply" : "dry-run",
        result: await seedProductionDetailRecoveryFiles({
          apply,
          authorsPath: resolvedAuthorsPath,
          ledger,
          manifest: parsedManifest,
          poemsPath: resolvedPoemsPath,
        }),
      });
    } finally {
      ledger.close();
    }
    return;
  }
  if (command === "init") {
    await mkdir(paths.root, { mode: 0o700, recursive: true });
    const ledger = Ledger.initialize(paths.database);
    try {
      ledger.pauseControls.read();
      readServiceEnabled(paths.root);
      let createdAttempts = false;
      try {
        await mkdir(paths.solAttempts, { mode: 0o700 });
        createdAttempts = true;
      } catch (error) {
        if (!(
          error instanceof Error &&
          "code" in error &&
          error.code === "EEXIST"
        ))
          throw error;
      }
      const index = resolve(paths.solAttempts, "operation-index");
      if (createdAttempts) await mkdir(index, { mode: 0o700 });
      const attemptsMetadata = await lstat(paths.solAttempts);
      const indexMetadata = await lstat(index);
      if (
        !attemptsMetadata.isDirectory() ||
        attemptsMetadata.isSymbolicLink() ||
        !indexMetadata.isDirectory() ||
        indexMetadata.isSymbolicLink()
      )
        throw new Error("INIT_UNSAFE_ATTEMPT_INDEX");
      new ArtifactStore(paths.artifacts);
      print({ command, initialized: true, paths, status: ledger.status() });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "health") {
    const format = option(commandArguments, "--format");
    if (format !== null && format !== "json")
      throw new Error("health supports only --format json");
    const health = await readPipelineHealth(
      resolve(paths.root, "health/latest.json"),
    );
    const quotaSignal = await readSolQuotaSignal(
      resolve(paths.root, "signals/sol-quota.json"),
    );
    const evaluation = evaluatePipelineHealth(health, Date.now(), quotaSignal);
    print({
      command,
      evaluation,
      health,
      quotaSignal,
    });
    if (commandArguments.includes("--fail-on-blocked"))
      process.exitCode = evaluation.exitCode;
    return;
  }

  if (command === "status") {
    const ledger = await openExisting(paths.database, { readonly: true });
    try {
      const stateInventory = await inspectStateInventory({
        ledger,
        root: paths.root,
      });
      const pauseState = ledger.pauseControls.read();
      const owner = await inspectRunOwner(resolve(paths.root, "RUN.lock"));
      print({
        command,
        paidWorkPaused: pauseState.paidWorkPaused,
        paths,
        paused: pauseState.paused,
        runLock: owner.lock,
        runtimeOwnerIssue: owner.issue,
        supervisorStatus: (await pathExists(resolve(paths.root, "status.json")))
          ? await readJson(resolve(paths.root, "status.json"))
          : null,
        status: ledger.status(),
        stateInventory,
      });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "pause" || command === "resume" || command === "pause-paid") {
    const state = PauseControls.withExisting(paths.root, (controls) =>
      controls.set(
        command === "pause-paid" ? "paid" : "global",
        command !== "resume",
      ),
    );
    print({ command, ...state, paths });
    return;
  }

  if (command === "resume-paid") {
    const maximumRaw = option(commandArguments, "--maximum-sol-operations");
    const maximumOperations = Number(maximumRaw);
    if (
      maximumRaw === null ||
      !Number.isSafeInteger(maximumOperations) ||
      maximumOperations <= 0 ||
      maximumOperations % 3 !== 0
    ) {
      throw new Error(
        "resume-paid requires --maximum-sol-operations as a positive multiple of 3",
      );
    }
    const ledger = await openExisting(paths.database);
    try {
      const paidUsageBudget = ledger.armSolPaidUsageBudget(
        maximumOperations,
        commandArguments.includes("--rearm"),
      );
      ledger.pauseControls.set("paid", false);
      print({ command, paidUsageBudget, paidWorkPaused: false, paths });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "clear-source-stop") {
    if (!commandArguments.includes("--confirm"))
      throw new Error("clear-source-stop requires --confirm");
    const ledger = await openExisting(paths.database);
    try {
      print({
        cleared: ledger.clearOriginStop(currentSource().origin),
        command,
        origin: currentSource().origin,
        status: ledger.status(),
      });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "seed-author") {
    const author = option(commandArguments, "--author");
    if (!author) throw new Error("seed-author requires --author URL");
    const ledger = await openExisting(paths.database);
    try {
      print({
        command,
        result: seedAuthorManifest(ledger, author),
        status: ledger.status(),
      });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "seed-authors") {
    const inputPath = option(commandArguments, "--input");
    if (!inputPath) throw new Error("seed-authors requires --input JSON file");
    const raw = await readJson(resolve(inputPath));
    const inventory = parseCatalogInventory(raw);
    const refreshGeneration = option(commandArguments, "--refresh-generation");
    const ledger = await openExisting(paths.database);
    try {
      const results = seedAuthorManifests(
        ledger,
        inventory,
        0,
        refreshGeneration ?? undefined,
      );
      print({
        command,
        declaredPoems: inventory.declaredPoems,
        duplicate: results.filter(({ inserted }) => !inserted).length,
        inserted: results.filter(({ inserted }) => inserted).length,
        status: ledger.status(),
        unknownPoemCounts: inventory.unknownPoemCounts,
      });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "seed-enrichment") {
    const provider = enrichmentProvider(commandArguments);
    const inputPath = option(commandArguments, "--input");
    if (!inputPath)
      throw new Error("seed-enrichment requires --input JSON file");
    const priorityRaw = option(commandArguments, "--priority");
    const priority = priorityRaw === null ? 0 : Number(priorityRaw);
    if (!Number.isSafeInteger(priority))
      throw new Error("--priority must be an integer");
    const raw = await readJson(resolve(inputPath));
    const inputs = (Array.isArray(raw) ? raw : [raw]).map((input) =>
      PoemEnrichmentInputSchema.parse(input),
    );
    const ledger = await openExisting(paths.database);
    const coordinator = new SolEnrichmentCoordinator({
      artifacts: new ArtifactStore(paths.artifacts),
      ledger,
      runner: new CodexSolRunner({
        operations: ledger.solOperations,
        attemptRoot: providerAttemptRoot(paths, provider),
        cwd: process.cwd(),
        provider,
      }),
    });
    try {
      const results = coordinator.seedMany(inputs, priority);
      print({
        command,
        duplicate: results.filter((result) => !result.inserted).length,
        inserted: results.filter((result) => result.inserted).length,
        paths,
        status: ledger.status(),
      });
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "prepare-import") {
    const artifactPath = option(commandArguments, "--artifact");
    const mappingPath = option(commandArguments, "--mapping");
    const observedAt = option(commandArguments, "--observed-at");
    const writerEpoch = Number(option(commandArguments, "--writer-epoch"));
    if (!artifactPath || !mappingPath || !observedAt) {
      throw new Error(
        "prepare-import requires --artifact, --mapping, and --observed-at",
      );
    }
    if (!Number.isSafeInteger(writerEpoch) || writerEpoch < 1) {
      throw new Error("--writer-epoch must be a positive integer");
    }
    print(
      prepareCollectedPoem(
        await readJson(resolve(artifactPath)),
        CatalogPoemMappingSchema.parse(await readJson(resolve(mappingPath))),
        observedAt,
        writerEpoch,
      ),
    );
    return;
  }

  if (command === "run-enrichment") {
    const provider = enrichmentProvider(commandArguments);
    const maximumRaw = option(commandArguments, "--max");
    const maximum = maximumRaw === null ? undefined : Number(maximumRaw);
    if (
      maximum !== undefined &&
      (!Number.isSafeInteger(maximum) || maximum <= 0)
    ) {
      throw new Error("--max must be a positive integer");
    }
    // The maintenance scope means strict exclusive job ownership, not only
    // migrations. It cannot reclaim an existing managed or standalone owner.
    const owner = await acquireRunLock(
      resolve(paths.root, "RUN.lock"),
      sha256(
        canonicalJson({
          command,
          provider,
          root: paths.root,
          source: currentSource(),
        }),
      ),
      new Date(),
      { recoverStale: false },
    );
    let ledger: Ledger | undefined;
    const controller = new AbortController();
    const abort = () => controller.abort(new Error("Operator stop"));
    process.once("SIGINT", abort);
    process.once("SIGTERM", abort);
    try {
      const activeLedger = await openExisting(paths.database);
      ledger = activeLedger;
      activeLedger.solOperations.assertImported();
      activeLedger.recoverExpired();
      const coordinator = new SolEnrichmentCoordinator({
        artifacts: new ArtifactStore(paths.artifacts),
        enforcePaidUsageBudget: true,
        ledger: activeLedger,
        runner: new CodexSolRunner({
          operations: activeLedger.solOperations,
          attemptRoot: providerAttemptRoot(paths, provider),
          cwd: process.cwd(),
          provider,
        }),
      });
      const result = await coordinator.run(controller.signal, {
        ...(maximum === undefined ? {} : { maximum }),
        paused: () => {
          const state = activeLedger.pauseControls.read();
          return state.paused || state.paidWorkPaused;
        },
      });
      print({ command, paths, result, status: activeLedger.status() });
    } finally {
      process.off("SIGINT", abort);
      process.off("SIGTERM", abort);
      // If resource closure fails, preserve the owner as failure evidence.
      ledger?.close();
      await owner.release();
    }
    return;
  }

  if (command === "verify") {
    const ledger = await openExisting(paths.database);
    try {
      const store = new ArtifactStore(paths.artifacts);
      const artifacts = await verifyArtifactInventory(
        store,
        ledger.referencedArtifactHashes(),
      );
      const { corrupt, missingOrCorruptReferences } = artifacts;
      const ledgerReport = ledger.doctor();
      print({
        artifacts,
        command,
        healthy:
          corrupt.length === 0 &&
          missingOrCorruptReferences.length === 0 &&
          ledgerReport.integrity === "ok",
        ledger: ledgerReport,
        paths,
      });
      if (corrupt.length > 0 || missingOrCorruptReferences.length > 0) {
        process.exitCode = 2;
      }
    } finally {
      ledger.close();
    }
    return;
  }

  if (command === "doctor") {
    const ledger = await openExisting(paths.database);
    try {
      const report = ledger.doctor(
        Date.now(),
        commandArguments.includes("--deep-integrity") ? "full" : "schema",
      );
      const stateInventory = await inspectStateInventory({
        ledger,
        root: paths.root,
      });
      print({ command, paths, report, stateInventory });
      if (
        report.integrity !== "ok" ||
        report.schemaVersion < 1 ||
        !stateInventory.codexScheduler.safeToOperate
      )
        process.exitCode = 2;
    } finally {
      ledger.close();
    }
    return;
  }

  throw new Error("Unknown command. Run `saqi-crawler --help` for usage.");
}

function locations(values: readonly string[]): Locations {
  const index = values.indexOf("--state-dir");
  const configuredRoot = index >= 0 ? values[index + 1] : ".saqi-runs";
  if (!configuredRoot) throw new Error("--state-dir requires a path");
  const root = resolve(configuredRoot);
  return {
    artifacts: resolve(root, "artifacts"),
    database: resolve(root, "ledger.sqlite3"),
    paidWorkPaused: resolve(root, "PAID_WORK_PAUSED"),
    paused: resolve(root, "PAUSED"),
    root,
    schedulerState: resolve(root, "sol-scheduler.json"),
    solAttempts: resolve(root, "sol-attempts"),
  };
}

function print(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function openExisting(
  path: string,
  options: Readonly<{ readonly?: boolean }> = {},
): Promise<Ledger> {
  try {
    await access(path);
  } catch {
    throw new Error(`Ledger does not exist: ${path}`);
  }
  return Ledger.open(path, options);
}

function option(values: readonly string[], name: string): null | string {
  const index = values.indexOf(name);
  if (index < 0) return null;
  const value = values[index + 1];
  if (!value || value.startsWith("--"))
    throw new Error(`${name} requires a value`);
  return value;
}

function enrichmentProvider(
  commandArguments: readonly string[],
): EnrichmentProvider {
  return EnrichmentProviderSchema.parse(
    option(commandArguments, "--provider") ?? "sol",
  );
}

function providerAttemptRoot(
  paths: Locations,
  _provider: EnrichmentProvider,
): string {
  return paths.solAttempts;
}

async function readJson(path: string): Promise<unknown> {
  return JSON.parse(await readFile(path, "utf8"));
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

try {
  await main(process.argv.slice(2));
} catch (error) {
  process.stderr.write(
    `${error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
}
