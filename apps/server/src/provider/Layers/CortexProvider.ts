import type {
  CortexSettings,
  ModelCapabilities,
  ProviderOptionSelection,
  ServerProviderAuth,
  ServerProviderModel,
  ServerProviderState,
} from "@t3tools/contracts";
import { ProviderDriverKind } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import type * as EffectAcpSchema from "effect-acp/schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { AcpSessionRuntime } from "../acp/AcpSessionRuntime.ts";
import { buildCortexAcpSpawnInput } from "../acp/CortexAcpSupport.ts";
import {
  buildServerProvider,
  collectStreamAsString,
  isCommandMissingCause,
  providerModelsFromSettings,
  type CommandResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("cortex");
const CORTEX_PRESENTATION = {
  displayName: "Cortex Code",
  badgeLabel: "Preview",
  showInteractionModeToggle: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
const CORTEX_ACP_MODEL_DISCOVERY_TIMEOUT_MS = 15_000;
const HEALTH_CHECK_TIMEOUT_MS = 8_000;

interface CortexSessionSelectOption {
  readonly value: string;
  readonly name: string;
}

interface CortexAcpDiscoveredModel {
  readonly slug: string;
  readonly name: string;
  readonly capabilities: ModelCapabilities;
}

export function resolveCortexAcpBaseModelId(model: string | null | undefined): string {
  const trimmed = model?.trim();
  const base = trimmed && trimmed.length > 0 ? trimmed : "auto";
  return base.includes("[") ? base.slice(0, base.indexOf("[")) : base;
}

export function resolveCortexAcpConfigUpdates(
  _configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
  _selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
): ReadonlyArray<{
  readonly configId: string;
  readonly value: string | boolean;
}> {
  return [];
}

function flattenSessionConfigSelectOptions(
  configOption: EffectAcpSchema.SessionConfigOption | undefined,
): ReadonlyArray<CortexSessionSelectOption> {
  if (!configOption || configOption.type !== "select") {
    return [];
  }
  return configOption.options.flatMap((entry) =>
    "value" in entry
      ? [
          {
            value: entry.value.trim(),
            name: entry.name.trim(),
          } satisfies CortexSessionSelectOption,
        ]
      : entry.options.map(
          (option) =>
            ({
              value: option.value.trim(),
              name: option.name.trim(),
            }) satisfies CortexSessionSelectOption,
        ),
  );
}

function findCortexModelConfigOption(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
): EffectAcpSchema.SessionConfigOption | undefined {
  return (
    configOptions.find((option) => option.category === "model") ??
    configOptions.find((option) => option.id.trim().toLowerCase() === "model") ??
    configOptions.find((option) => option.name.trim().toLowerCase() === "model")
  );
}

function buildCortexDiscoveredModels(
  discoveredModels: ReadonlyArray<CortexAcpDiscoveredModel>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  return discoveredModels.flatMap((model) => {
    if (!model.slug || seen.has(model.slug)) {
      return [];
    }
    seen.add(model.slug);
    return [
      {
        slug: model.slug,
        name: model.name,
        isCustom: false,
        capabilities: model.capabilities,
      } satisfies ServerProviderModel,
    ];
  });
}

export function buildCortexDiscoveredModelsFromConfigOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
): ReadonlyArray<ServerProviderModel> {
  if (!configOptions || configOptions.length === 0) {
    return [];
  }

  const modelOption = findCortexModelConfigOption(configOptions);
  const modelChoices = flattenSessionConfigSelectOptions(modelOption);
  if (!modelOption || modelChoices.length === 0) {
    return [];
  }

  return buildCortexDiscoveredModels(
    modelChoices.map((modelChoice) => ({
      slug: modelChoice.value.trim(),
      name: modelChoice.name.trim(),
      capabilities: EMPTY_CAPABILITIES,
    })),
  );
}

const makeCortexAcpProbeRuntime = (
  cortexSettings: CortexSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const cwd = process.cwd();
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        spawn: buildCortexAcpSpawnInput(cortexSettings, cwd, environment),
        cwd,
        clientInfo: { name: "t3-code-cortex-provider-probe", version: "0.0.0" },
        authMethodId: null,
      }).pipe(Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner))),
    );
    return yield* Effect.service(AcpSessionRuntime).pipe(Effect.provide(acpContext));
  });

const withCortexAcpProbeRuntime = <A, E, R>(
  cortexSettings: CortexSettings,
  useRuntime: (acp: AcpSessionRuntime["Service"]) => Effect.Effect<A, E, R>,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  makeCortexAcpProbeRuntime(cortexSettings, environment).pipe(
    Effect.flatMap(useRuntime),
    Effect.scoped,
  );

export const discoverCortexModelsViaAcp = (
  cortexSettings: CortexSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  withCortexAcpProbeRuntime(
    cortexSettings,
    (acp) =>
      Effect.map(acp.start(), (started) =>
        buildCortexDiscoveredModelsFromConfigOptions(
          started.sessionSetupResult.configOptions ?? [],
        ),
      ),
    environment,
  );

export function getCortexFallbackModels(
  cortexSettings: Pick<CortexSettings, "customModels">,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings([], PROVIDER, cortexSettings.customModels, EMPTY_CAPABILITIES);
}

export function buildInitialCortexProviderSnapshot(
  cortexSettings: CortexSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = getCortexFallbackModels(cortexSettings);

    if (!cortexSettings.enabled) {
      return buildServerProvider({
        presentation: CORTEX_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Cortex Code is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: CORTEX_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Cortex Code availability...",
      },
    });
  });
}

export interface CortexHealthResult {
  readonly version: string | null;
  readonly status: Exclude<ServerProviderState, "disabled">;
  readonly auth: ServerProviderAuth;
  readonly message?: string;
}

function parseVersionOutput(result: CommandResult): string | null {
  const combined = `${result.stdout}\n${result.stderr}`;
  const match = /(?:cortex(?:\s+code)?\s*)?v?(\d+(?:\.\d+)+(?:[-+._a-zA-Z0-9]*)?)/i.exec(combined);
  return match?.[1]?.trim() ?? null;
}

function parseConnectionStatusOutput(
  result: CommandResult,
  requestedConnection: string | undefined,
): Pick<CortexHealthResult, "auth" | "status" | "message"> {
  const combined = `${result.stdout}\n${result.stderr}`.trim();
  if (result.code !== 0) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message:
        "Cortex Code could not list Snowflake connections. Run `cortex connections list` in a terminal and configure a Snowflake connection.",
    };
  }

  try {
    const parsed = JSON.parse(combined) as unknown;
    if (typeof parsed === "object" && parsed !== null) {
      const record = parsed as {
        readonly active_connection?: unknown;
        readonly connections?: unknown;
      };
      const connections =
        typeof record.connections === "object" && record.connections !== null
          ? Object.keys(record.connections)
          : [];
      const activeConnection =
        typeof record.active_connection === "string" && record.active_connection.trim()
          ? record.active_connection.trim()
          : undefined;
      const connectionName = requestedConnection || activeConnection;
      if (connections.length === 0) {
        return {
          status: "error",
          auth: { status: "unauthenticated" },
          message:
            "Cortex Code has no Snowflake connections configured. Add one to `~/.snowflake/connections.toml` or use `cortex connections` before starting a session.",
        };
      }
      if (requestedConnection && !connections.includes(requestedConnection)) {
        return {
          status: "error",
          auth: { status: "unauthenticated" },
          message: `Cortex Code connection ${JSON.stringify(requestedConnection)} was not found. Check the provider settings or run \`cortex connections list\`.`,
        };
      }
      return {
        status: "ready",
        auth: {
          status: "authenticated",
          ...(connectionName
            ? { type: connectionName, label: `Snowflake ${connectionName}` }
            : { label: "Snowflake connection" }),
        },
      };
    }
  } catch {
    // Fall back to text parsing below for older CLI formats.
  }

  const lowerOutput = combined.toLowerCase();
  if (lowerOutput.includes("no connections") || lowerOutput.includes("not configured")) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: "Cortex Code has no Snowflake connections configured.",
    };
  }
  const connectionMatch =
    /(?:active[_\s-]?connection|connection)\s*[:=]\s*([^\r\n]+)/i.exec(combined) ??
    /using\s+([A-Za-z0-9_.-]+)\s+connection/i.exec(combined);
  return {
    status: "ready",
    auth: {
      status: "authenticated",
      ...(connectionMatch?.[1]?.trim()
        ? { type: connectionMatch[1].trim(), label: `Snowflake ${connectionMatch[1].trim()}` }
        : { label: "Snowflake connection" }),
    },
  };
}

const runCortexCommand = (
  cortexSettings: CortexSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const command = ChildProcess.make(cortexSettings.binaryPath, [...args], {
      env: environment,
      shell: process.platform === "win32",
    });

    const child = yield* spawner.spawn(command);
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );

    return { stdout, stderr, code: exitCode } satisfies CommandResult;
  }).pipe(Effect.scoped);

export const checkCortexProviderStatus = Effect.fn("checkCortexProviderStatus")(function* (
  cortexSettings: CortexSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = getCortexFallbackModels(cortexSettings);

  if (!cortexSettings.enabled) {
    return buildServerProvider({
      presentation: CORTEX_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Cortex Code is disabled in T3 Code settings.",
      },
    });
  }

  const versionProbe = yield* runCortexCommand(cortexSettings, ["--version"], environment).pipe(
    Effect.timeoutOption(HEALTH_CHECK_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      presentation: CORTEX_PRESENTATION,
      enabled: cortexSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Cortex Code CLI (`cortex`) is not installed or not on PATH."
          : `Failed to execute Cortex Code CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      presentation: CORTEX_PRESENTATION,
      enabled: cortexSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Cortex Code CLI is installed but timed out while running `cortex --version`.",
      },
    });
  }

  const version = parseVersionOutput(versionProbe.success.value);
  const requestedConnection = cortexSettings.connectionName.trim() || undefined;
  const connectionProbe = yield* runCortexCommand(
    cortexSettings,
    ["connections", "list"],
    environment,
  ).pipe(Effect.timeoutOption(HEALTH_CHECK_TIMEOUT_MS), Effect.result);
  const parsedAuth =
    Result.isSuccess(connectionProbe) && Option.isSome(connectionProbe.success)
      ? parseConnectionStatusOutput(connectionProbe.success.value, requestedConnection)
      : ({
          status: "warning",
          auth: { status: "unknown" },
          message: "Could not verify Cortex Code Snowflake connection status.",
        } satisfies Pick<CortexHealthResult, "auth" | "status" | "message">);

  let discoveredModels = Option.none<ReadonlyArray<ServerProviderModel>>();
  let discoveryWarning: string | undefined;
  if (parsedAuth.auth.status !== "unauthenticated") {
    const discoveryExit = yield* Effect.exit(
      discoverCortexModelsViaAcp(cortexSettings, environment).pipe(
        Effect.timeoutOption(CORTEX_ACP_MODEL_DISCOVERY_TIMEOUT_MS),
      ),
    );
    if (Exit.isFailure(discoveryExit)) {
      yield* Effect.logWarning("Cortex ACP model discovery failed", {
        cause: Cause.pretty(discoveryExit.cause),
      });
      discoveryWarning =
        "Cortex ACP model discovery failed. Check the Snowflake connection and server logs.";
    } else if (Option.isNone(discoveryExit.value)) {
      discoveryWarning = `Cortex ACP model discovery timed out after ${CORTEX_ACP_MODEL_DISCOVERY_TIMEOUT_MS}ms.`;
    } else if (discoveryExit.value.value.length === 0) {
      discoveryWarning = "Cortex ACP model discovery returned no built-in models.";
    } else {
      discoveredModels = discoveryExit.value;
    }
  }

  const message = [parsedAuth.message, discoveryWarning]
    .map((part) => part?.trim())
    .filter((part): part is string => Boolean(part))
    .join(" ");

  return buildServerProvider({
    presentation: CORTEX_PRESENTATION,
    enabled: cortexSettings.enabled,
    checkedAt,
    models: providerModelsFromSettings(
      Option.getOrElse(
        Option.filter(discoveredModels, (models) => models.length > 0),
        () => [] as const,
      ),
      PROVIDER,
      cortexSettings.customModels,
      EMPTY_CAPABILITIES,
    ),
    probe: {
      installed: true,
      version,
      status: discoveryWarning && parsedAuth.status === "ready" ? "warning" : parsedAuth.status,
      auth: parsedAuth.auth,
      ...(message ? { message } : {}),
    },
  });
});

export const enrichCortexSnapshot = (): Effect.Effect<void> => Effect.void;
