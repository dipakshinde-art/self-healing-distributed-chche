import "dotenv/config";
import { z } from "zod";
import { NodeConfig } from "../src/types";

const schema = z.object({
  NODE_ID:                z.string().default("node-1"),
  NODE_HOST:              z.string().default("127.0.0.1"),
  NODE_PORT:              z.coerce.number().default(3001),
  NODE_GOSSIP_PORT:       z.coerce.number().default(4001),
  SEED_HOST:              z.string().default("127.0.0.1"),
  SEED_PORT:              z.coerce.number().default(3001),
  REPLICATION_FACTOR:     z.coerce.number().default(2),
  VNODE_COUNT:            z.coerce.number().default(150),
  SUSPECT_TIMEOUT_MS:     z.coerce.number().default(3000),
  DEAD_TIMEOUT_MS:        z.coerce.number().default(6000),
  GOSSIP_INTERVAL_MS:     z.coerce.number().default(1000),
  GOSSIP_FANOUT:          z.coerce.number().default(3),
  TTL_SWEEP_INTERVAL_MS:  z.coerce.number().default(5000),
  EVICTION_POLICY:        z.enum(["none", "lru", "lfu"]).default("none"),
  EVICTION_THRESHOLD_MB:  z.coerce.number().default(256),
  EVICTION_SWEEP_INTERVAL_MS: z.coerce.number().default(5000),
  MAX_RETRIES:            z.coerce.number().default(1),
  RETRY_DELAY_MS:         z.coerce.number().default(50),
  LOG_LEVEL:              z.string().default("info"),
  FORWARD_TIMEOUT_MS:     z.coerce.number().default(1500),
});

export function loadConfig(): NodeConfig {
  const env = schema.parse(process.env);

  return {
    nodeId:               env.NODE_ID,
    host:                 env.NODE_HOST,
    port:                 env.NODE_PORT,
    gossipPort:           env.NODE_GOSSIP_PORT,
    seedHost:             env.SEED_HOST,
    seedPort:             env.SEED_PORT,
    replicationFactor:    env.REPLICATION_FACTOR,
    vnodeCount:           env.VNODE_COUNT,
    suspectTimeoutMs:     env.SUSPECT_TIMEOUT_MS,
    deadTimeoutMs:        env.DEAD_TIMEOUT_MS,
    gossipIntervalMs:     env.GOSSIP_INTERVAL_MS,
    gossipFanout:         env.GOSSIP_FANOUT,
    ttlSweepIntervalMs:   env.TTL_SWEEP_INTERVAL_MS,
    evictionPolicy:       env.EVICTION_POLICY,
    evictionThresholdMb:  env.EVICTION_THRESHOLD_MB,
    evictionSweepIntervalMs: env.EVICTION_SWEEP_INTERVAL_MS,
    maxRetries:           env.MAX_RETRIES,
    retryDelayMs:         env.RETRY_DELAY_MS,
    logLevel:             env.LOG_LEVEL,
    forwardTimeoutMs:     env.FORWARD_TIMEOUT_MS,
  };
}
