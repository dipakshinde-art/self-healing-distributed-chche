export type NodeStatus = "ALIVE" | "SUSPECT" | "DEAD";

export type EvictionPolicy = "none" | "lru" | "lfu";

export type ConsistencyMode = "eventual" | "strong";

export interface CacheEntry {
  key: string;
  value: string | number | Buffer;
  expiresAt: number | null; // absolute Unix ms, null = no TTL
  version: number;
  nodeId: string;
  createdAt: number;
}

export interface MembershipEntry {
  nodeId: string;
  host: string;
  port: number;       // HTTP port
  gossipPort: number; // UDP port
  status: NodeStatus;
  term: number;       // heartbeat counter
  lastSeen: number;   // Unix ms
}

export interface GossipMessage {
  type: "HEARTBEAT" | "STATE_UPDATE" | "JOIN" | "LEAVE";
  from: string;
  term: number;
  members: MembershipEntry[];
  timestamp: number;
}

export interface VNode {
  position: number; // uint32 on the ring
  nodeId: string;
}

export interface SetOptions {
  ttl?: number; // seconds
  consistency?: ConsistencyMode;
}

export interface GetOptions {
  consistency?: ConsistencyMode;
}

export interface NodeConfig {
  nodeId: string;
  host: string;
  port: number;
  gossipPort: number;
  seedHost: string;
  seedPort: number;
  replicationFactor: number;
  vnodeCount: number;
  suspectTimeoutMs: number;
  deadTimeoutMs: number;
  gossipIntervalMs: number;
  gossipFanout: number;
  ttlSweepIntervalMs: number;
  evictionPolicy: EvictionPolicy;
  evictionThresholdMb: number;
  maxRetries: number;
  retryDelayMs: number;
  logLevel: string;
}

export interface ClusterMetrics {
  totalRequests: number;
  failedRequests: number;
  retryCount: number;
  latencyP50: number;
  latencyP99: number;
}

export interface ReplicationPayload {
  key: string;
  entry: CacheEntry;
  sourceNodeId: string;
}

export interface MigrationBatch {
  entries: CacheEntry[];
  sourceNodeId: string;
  targetNodeId: string;
}
