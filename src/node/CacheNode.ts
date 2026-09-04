import http from "http";
import fs from "fs";
import path from "path";
import { loadConfig } from "../../config/cluster.config";
import { CacheStore } from "../storage/CacheStore";
import { createEvictionTracker } from "../storage/EvictionPolicy";
import { EvictionSweeper } from "../storage/EvictionSweeper";
import { TTLEngine } from "../ttl/TTLEngine";
import { HashRing } from "../hashing/HashRing";
import { MembershipList } from "../gossip/MembershipList";
import { NodeStateManager } from "../gossip/NodeStateManager";
import { GossipAgent } from "../gossip/GossipAgent";
import { ReplicationManager } from "../replication/ReplicationManager";
import { KeyMigrator } from "../replication/KeyMigrator";
import { NodeServer } from "./NodeServer";
import { createLogger } from "../utils/logger";
import { withRetry } from "../utils/retry";
import { JoinRequest, JoinResponse, MembershipEntry } from "../types";

// Cluster bootstrap races against the seed node's own startup (all nodes are
// spawned concurrently by `npm run start:cluster`), so retry generously —
// this is unrelated to the client-facing MAX_RETRIES/RETRY_DELAY_MS config.
const JOIN_MAX_RETRIES = 20;
const JOIN_RETRY_DELAY_MS = 500;

const PID_DIR = path.join(process.cwd(), ".pids");

async function main() {
  const cfg = loadConfig();
  const logger = createLogger(cfg.nodeId, cfg.logLevel);

  logger.info({ nodeId: cfg.nodeId, port: cfg.port }, "Starting cache node");

  // Core storage
  const store = new CacheStore();
  const ttl = new TTLEngine(store, cfg.ttlSweepIntervalMs, logger);

  // Cluster ring + membership
  const ring = new HashRing(cfg.vnodeCount);
  const membership = new MembershipList();
  const migrator = new KeyMigrator(store, cfg.nodeId, logger);
  const eviction = createEvictionTracker(cfg.evictionPolicy);
  const evictionSweeper = new EvictionSweeper(
    store,
    eviction,
    cfg.evictionThresholdMb,
    cfg.evictionSweepIntervalMs,
    logger
  );

  // Register self
  membership.add({
    nodeId: cfg.nodeId,
    host: cfg.host,
    port: cfg.port,
    gossipPort: cfg.gossipPort,
    status: "ALIVE",
    term: 0,
    lastSeen: Date.now(),
  });
  ring.addNode(membership.get(cfg.nodeId)!);

  // Migrates this node's own share of keys to `target` — used both when a
  // node recovers from DEAD (rejoin) and when a brand-new node is discovered,
  // whether that discovery came from a direct join or from gossip.
  function migrateOwnedKeysTo(target: MembershipEntry): void {
    const keysForTarget = ring.getKeysForNode(store.keys(), target.nodeId);
    if (keysForTarget.length === 0) return;
    migrator.migrateKeys(keysForTarget, target).catch((err) =>
      logger.warn({ err, target: target.nodeId }, "Migration failed")
    );
  }

  // State machine + gossip
  const stateManager = new NodeStateManager(
    membership,
    cfg.suspectTimeoutMs,
    cfg.deadTimeoutMs,
    logger
  );

  stateManager.on("node:dead", (nodeId: string) => {
    logger.error({ nodeId }, "Removing dead node from ring");
    ring.removeNode(nodeId);
  });

  stateManager.on("node:alive", (nodeId: string) => {
    const entry = membership.get(nodeId);
    if (entry) {
      logger.info({ nodeId }, "Re-adding recovered node to ring");
      ring.addNode(entry);
      migrateOwnedKeysTo(entry);
    }
  });

  const gossip = new GossipAgent(
    cfg.nodeId,
    cfg.host,
    cfg.gossipPort,
    membership,
    stateManager,
    cfg.gossipFanout,
    cfg.gossipIntervalMs,
    logger
  );

  // A node only ever gets added to *this* node's ring directly by a join
  // handshake (seed admitting a joiner, or the joiner adopting the seed's
  // response) — a third node that never talks to the joiner would otherwise
  // never learn about it. Gossip is the only thing that reaches every node
  // eventually, so ring membership (and the migration that follows a genuine
  // new arrival) has to be kept in sync from it too.
  const knownNodeIds = new Set(ring.getNodes().map((n) => n.nodeId));
  gossip.on("membership:changed", (changedIds: string[]) => {
    for (const nodeId of changedIds) {
      if (nodeId === cfg.nodeId) continue;
      const entry = membership.get(nodeId);
      if (!entry) continue;

      if (entry.status === "DEAD") {
        ring.removeNode(nodeId);
        knownNodeIds.delete(nodeId);
        continue;
      }

      if (entry.status === "ALIVE") {
        const isNewToRing = !knownNodeIds.has(nodeId);
        ring.addNode(entry);
        knownNodeIds.add(nodeId);
        if (isNewToRing) {
          logger.info({ nodeId }, "Learned about node via gossip, syncing ring");
          migrateOwnedKeysTo(entry);
        }
      }
    }
  });

  // Replication
  const replication = new ReplicationManager(cfg.nodeId, logger);

  // HTTP server
  const server = new NodeServer(
    cfg.nodeId,
    cfg.port,
    store,
    ring,
    replication,
    cfg.replicationFactor,
    membership,
    migrator,
    eviction,
    logger,
    cfg.forwardTimeoutMs
  );

  // Bootstrap: join seed node if we're not the seed
  if (cfg.seedPort !== cfg.port || cfg.seedHost !== cfg.host) {
    await joinSeedNode(cfg, ring, membership, logger);
    for (const entry of membership.getAll()) knownNodeIds.add(entry.nodeId);
  }

  // Start everything
  ttl.start();
  if (cfg.evictionPolicy !== "none") evictionSweeper.start();
  await gossip.start();
  await server.start();
  writePidFile(cfg.nodeId);

  logger.info("Cache node fully started");

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    logger.info("Shutting down...");
    gossip.stop();
    ttl.stop();
    evictionSweeper.stop();
    removePidFile(cfg.nodeId);
    await server.stop();
    process.exit(0);
  });
}

// Lets scripts/kill-node.ts find and terminate a specific node process by
// nodeId without parsing `netstat`/`tasklist` output — works identically on
// Windows, macOS, and Linux.
function writePidFile(nodeId: string): void {
  fs.mkdirSync(PID_DIR, { recursive: true });
  fs.writeFileSync(path.join(PID_DIR, `${nodeId}.pid`), String(process.pid));
}

function removePidFile(nodeId: string): void {
  try {
    fs.unlinkSync(path.join(PID_DIR, `${nodeId}.pid`));
  } catch {
    // already gone — fine
  }
}

// Joiner's side of cluster bootstrap: send our own membership entry to the
// seed's /internal/join, then adopt every peer it hands back into our own
// membership list + ring. Retries with backoff since the seed may not be
// listening yet (all nodes are spawned concurrently by start:cluster).
async function joinSeedNode(
  cfg: ReturnType<typeof loadConfig>,
  ring: HashRing,
  membership: MembershipList,
  logger: ReturnType<typeof createLogger>
): Promise<void> {
  logger.info({ seedHost: cfg.seedHost, seedPort: cfg.seedPort }, "Joining cluster via seed node");

  const self = membership.get(cfg.nodeId)!;
  const response = await withRetry(
    () => sendJoinRequest(cfg.seedHost, cfg.seedPort, self),
    JOIN_MAX_RETRIES,
    JOIN_RETRY_DELAY_MS
  );

  for (const member of response.members) {
    if (member.nodeId === cfg.nodeId) continue;
    membership.add(member);
    ring.addNode(member);
  }

  logger.info({ peers: response.members.length - 1 }, "Joined cluster");
}

function sendJoinRequest(host: string, port: number, entry: MembershipEntry): Promise<JoinResponse> {
  const body = JSON.stringify({ entry } satisfies JoinRequest);

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host,
        port,
        path: "/internal/join",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
        timeout: 5000,
        agent: false, // avoid a stale pooled socket to a seed that restarted on the same port
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          if (res.statusCode !== 200) return reject(new Error(`Join failed: HTTP ${res.statusCode}`));
          try {
            resolve(JSON.parse(data) as JoinResponse);
          } catch (err) {
            reject(err as Error);
          }
        });
      }
    );
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("Join request timed out"));
    });
    req.write(body);
    req.end();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
