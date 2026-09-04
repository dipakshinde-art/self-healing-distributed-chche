import { Command } from "commander";
import http from "http";
import path from "path";
import { ClusterManager, NodeSpawnConfig } from "../cluster/ClusterManager";
import { createLogger } from "../utils/logger";
import { MembershipEntry } from "../types";

export function formatStatusTable(nodeId: string, keys: number, members: MembershipEntry[]): string {
  const header = `Node: ${nodeId}  Keys: ${keys}\n`;
  const rows = members
    .map(
      (m) =>
        `  ${m.nodeId.padEnd(12)} ${`${m.host}:${m.port}`.padEnd(22)} ${m.status.padEnd(8)} term=${m.term}`
    )
    .join("\n");
  return header + rows;
}

export interface AddNodeArgs {
  nodeId: string;
  port: string | number;
  gossipPort: string | number;
  seedHost: string;
  seedPort: string | number;
  host?: string;
}

export function buildSpawnConfig(opts: AddNodeArgs): NodeSpawnConfig {
  return {
    nodeId: opts.nodeId,
    host: opts.host ?? "127.0.0.1",
    port: Number(opts.port),
    gossipPort: Number(opts.gossipPort),
    seedHost: opts.seedHost,
    seedPort: Number(opts.seedPort),
  };
}

interface StatusResponse {
  nodeId: string;
  keys: number;
  members: MembershipEntry[];
}

function fetchStatus(host: string, port: number): Promise<StatusResponse> {
  return new Promise((resolve, reject) => {
    const req = http.request({ host, port, path: "/status", method: "GET", timeout: 3000 }, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("Request timed out"));
    });
    req.end();
  });
}

function main(): void {
  const program = new Command();
  program.name("cache-cli").description("Cluster management CLI for the self-healing distributed cache");

  program
    .command("status")
    .description("Show a node's live membership view")
    .option("--host <host>", "node host", "127.0.0.1")
    .option("--port <port>", "node HTTP port", "3001")
    .action(async (opts: { host: string; port: string }) => {
      try {
        const status = await fetchStatus(opts.host, Number(opts.port));
        console.log(formatStatusTable(status.nodeId, status.keys, status.members));
      } catch (err) {
        console.error("Failed to fetch status:", (err as Error).message);
        process.exitCode = 1;
      }
    });

  program
    .command("add-node")
    .description("Spawn a new cache node process and have it join the cluster via a seed")
    .requiredOption("--node-id <id>", "unique node id")
    .requiredOption("--port <port>", "HTTP port for the new node")
    .requiredOption("--gossip-port <port>", "gossip UDP port for the new node")
    .option("--seed-host <host>", "seed node host", "127.0.0.1")
    .requiredOption("--seed-port <port>", "seed node HTTP port")
    .option("--host <host>", "host the new node binds to", "127.0.0.1")
    .action((opts: AddNodeArgs) => {
      const logger = createLogger("cli", "info");
      const manager = new ClusterManager(path.join(__dirname, "..", "node", "CacheNode.ts"), logger);
      const config = buildSpawnConfig(opts);
      manager.spawnNode(config);
      console.log(
        `Spawned ${config.nodeId} on port ${config.port} (gossip ${config.gossipPort}), ` +
          `joining via seed ${config.seedHost}:${config.seedPort}`
      );
    });

  program.parse(process.argv);
}

if (require.main === module) {
  main();
}
