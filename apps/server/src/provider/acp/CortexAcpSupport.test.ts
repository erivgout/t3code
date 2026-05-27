import { describe, expect, it } from "vitest";
import { buildCortexAcpSpawnInput } from "./CortexAcpSupport.ts";

describe("buildCortexAcpSpawnInput", () => {
  it("launches Cortex Code in ACP mode with a workdir", () => {
    expect(
      buildCortexAcpSpawnInput(
        {
          binaryPath: "cortex",
          connectionName: "",
          defaultModel: "",
          bypass: false,
        },
        "/repo",
      ),
    ).toEqual({
      command: "cortex",
      args: ["acp", "serve", "-w", "/repo"],
      cwd: "/repo",
    });
  });

  it("includes connection, model, bypass, and env when configured", () => {
    const env = { SNOWFLAKE_ACCOUNT: "example" };
    expect(
      buildCortexAcpSpawnInput(
        {
          binaryPath: "/bin/cortex",
          connectionName: "analytics",
          defaultModel: "claude-sonnet-4-6",
          bypass: true,
        },
        "/repo",
        env,
      ),
    ).toEqual({
      command: "/bin/cortex",
      args: [
        "acp",
        "serve",
        "-c",
        "analytics",
        "-m",
        "claude-sonnet-4-6",
        "-w",
        "/repo",
        "--bypass",
      ],
      cwd: "/repo",
      env,
    });
  });
});
