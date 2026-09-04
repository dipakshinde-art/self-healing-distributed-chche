import { ChildProcess, spawn } from "child_process";
import path from "path";
import { Logger } from "../utils/logger";

export interface NodeSpawnConfig {
  nodeId: string;
  host: string;
  port: number;
  gossipPort: number;
  seedHost: string;
  seedPort: number;
}

// Spawns and manages cache node child processes
export class ClusterManager {
  private processes = new Map<string, ChildProcess>();

  constructor(
    private scriptPath: string, // path to src/node/CacheNode.ts, run via the local tsx binary
    private logger: Logger
  ) {}

  spawnNode(config: NodeSpawnConfig): void {
    const env: Record<string, string> = {
      ...process.env as Record<string, string>,
      NODE_ID: config.nodeId,
      NODE_HOST: config.host,
      NODE_PORT: String(config.port),
      NODE_GOSSIP_PORT: String(config.gossipPort),
      SEED_HOST: config.seedHost,
      SEED_PORT: String(config.seedPort),
    };

    const tsxBin = path.join(
      process.cwd(),
      "node_modules",
      ".bin",
      process.platform === "win32" ? "tsx.cmd" : "tsx"
    );

    const child = spawn(tsxBin, [this.scriptPath], {
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    child.stdout?.on("data", (d) => process.stdout.write(`[${config.nodeId}] ${d}`));
    child.stderr?.on("data", (d) => process.stderr.write(`[${config.nodeId}] ${d}`));

    child.on("exit", (code) => {
      this.logger.warn({ nodeId: config.nodeId, code }, "Node process exited");
      this.processes.delete(config.nodeId);
    });

    this.processes.set(config.nodeId, child);
    this.logger.info({ nodeId: config.nodeId, port: config.port }, "Node process spawned");
  }

  killNode(nodeId: string): void {
    const proc = this.processes.get(nodeId);
    if (proc) {
      proc.kill("SIGKILL");
      this.processes.delete(nodeId);
      this.logger.warn({ nodeId }, "Node process killed");
    }
  }

  stopAll(): void {
    for (const [nodeId, proc] of this.processes) {
      proc.kill("SIGTERM");
      this.logger.info({ nodeId }, "Node process stopped");
    }
    this.processes.clear();
  }

  getRunningNodes(): string[] {
    return Array.from(this.processes.keys());
  }
}
