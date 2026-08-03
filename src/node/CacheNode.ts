import { loadConfig } from "../../config/cluster.config";
import { CacheStore } from "../storage/CacheStore";
import { TTLEngine } from "../ttl/TTLEngine";
import { HashRing } from "../hashing/HashRing";
import { MembershipList } from "../gossip/MembershipList";
import { NodeStateManager } from "../gossip/NodeStateManager";
import { GossipAgent } from "../gossip/GossipAgent";
import { ReplicationManager } from "../replication/ReplicationManager";
import { KeyMigrator } from "../replication/KeyMigrator";
import { NodeServer } from "./NodeServer";
import { createLogger } from "../utils/logger";

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
      // Trigger key migration back to rejoining node
      const migrator = new KeyMigrator(store, cfg.nodeId, logger);
      const myKeys = store.keys();
      const keysForNode = ring.getKeysForNode(myKeys, nodeId);
      if (keysForNode.length > 0) {
        migrator.migrateKeys(keysForNode, entry).catch((err) =>
          logger.warn({ err }, "Migration on rejoin failed")
        );
      }
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
    logger
  );

  // Bootstrap: join seed node if we're not the seed
  if (cfg.seedPort !== cfg.port || cfg.seedHost !== cfg.host) {
    await joinSeedNode(cfg, ring, membership, logger);
  }

  // Start everything
  ttl.start();
  await gossip.start();
  await server.start();

  logger.info("Cache node fully started");

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    logger.info("Shutting down...");
    gossip.stop();
    ttl.stop();
    await server.stop();
    process.exit(0);
  });
}

async function joinSeedNode(
  cfg: ReturnType<typeof loadConfig>,
  ring: HashRing,
  membership: MembershipList,
  logger: ReturnType<typeof createLogger>
) {
  // TODO Phase 3: send JOIN request to seed node, receive full membership list
  logger.info({ seedHost: cfg.seedHost, seedPort: cfg.seedPort }, "Joining cluster via seed node");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
