# Self-Healing Distributed Cache

A Redis-like distributed cache built from scratch in TypeScript — sharded, replicated,
and able to survive node failure live without dropping client requests.

This is not a wrapper around Redis. It implements the mechanisms that power real
production caches: consistent hashing, gossip-based failure detection, replica
failover, and zero-downtime rebalancing — all on raw Node.js `net`/`dgram`/`http`
modules.

## Why this exists

Anyone can call `SET`/`GET` on Redis. This project builds the system underneath
it: a cluster of independent nodes that discover each other, detect failures
without a central coordinator, replicate data, and reshuffle keys as nodes join
or leave — all while continuing to serve traffic.

## Use case

Run a cluster of cache nodes on your machine (or across hosts), write to any
node via the cluster client, and the cluster:

- Routes each key to its owning node using consistent hashing (minimal
  reshuffling when nodes are added/removed).
- Replicates each key to `RF-1` additional nodes, so a read still succeeds if
  the primary holder of that key goes down.
- Detects a dead node automatically via gossip heartbeats (no manual
  intervention) and reroutes traffic to replicas.
- Streams keys back to a recovered or newly joined node with zero downtime.
- Expires keys via TTL consistently across all replicas.

The end-to-end proof: kill a random node process while a load test is hitting
the cluster, and zero client requests fail (beyond one configurable retry).

## Tech stack

| Concern           | Choice                                   |
|--------------------|-------------------------------------------|
| Runtime            | Node.js v20+ (LTS)                        |
| Language           | TypeScript 5.x                            |
| Client-facing API  | HTTP (`http` module)                      |
| Node-to-node       | TCP (`net` module)                        |
| Gossip / heartbeat | UDP (`dgram` module)                      |
| In-memory store    | `Map<string, CacheEntry>` per node         |
| Consistent hashing | MurmurHash3 (`murmurhash3js`)             |
| Serialization      | MessagePack (`msgpackr`)                  |
| Config             | `dotenv` + `zod` (typed, validated)       |
| Logging            | Pino (structured JSON logs)               |
| Testing            | Jest + `ts-jest`                          |
| Dev tooling        | `tsx`, `concurrently`, `cross-env`        |

No framework hides the mechanics — every byte of the cluster protocol is owned
by this codebase.

## Project structure

```
src/
├── node/            CacheNode bootstrap + NodeServer (client-facing HTTP API)
├── cluster/         ClusterManager — spawns/manages node processes
├── hashing/         HashRing (consistent hashing) + MurmurHash3 wrapper
├── replication/     ReplicationManager + KeyMigrator (rebalancing)
├── gossip/          GossipAgent, NodeStateManager, MembershipList
├── storage/         CacheStore (Map-based) + EvictionPolicy (LRU/LFU)
├── ttl/             TTLEngine — lazy eviction + background sweep
├── client/          CacheClient SDK (set/get/del with retry)
├── utils/           Pino logger, retry with backoff
└── types/           Shared TypeScript types
tests/
├── unit/            Per-module unit tests
config/              Cluster config schema (zod)
scripts/             load-test.ts — concurrent SET/GET load generator
```

## Getting started

### Prerequisites

- Node.js v20+
- npm

### Install

```bash
npm install
```

### Configure

Copy the example env file and adjust as needed:

```bash
cp .env.example .env
```

Key environment variables:

| Variable               | Default       | Meaning                                   |
|-------------------------|---------------|--------------------------------------------|
| `NODE_ID`               | `node-1`      | Unique id for this node                    |
| `NODE_HOST`             | `127.0.0.1`   | Host this node binds to                    |
| `NODE_PORT`             | `3001`        | Client-facing HTTP port                    |
| `NODE_GOSSIP_PORT`      | `4001`        | UDP port for gossip heartbeats             |
| `SEED_HOST` / `SEED_PORT` | `127.0.0.1` / `3001` | Address of the seed node to join via |
| `REPLICATION_FACTOR`    | `2`           | Number of nodes each key is stored on      |
| `VNODE_COUNT`           | `150`         | Virtual nodes per physical node on the ring |
| `SUSPECT_TIMEOUT_MS`    | `3000`        | No heartbeat for this long → `SUSPECT`     |
| `DEAD_TIMEOUT_MS`       | `6000`        | No heartbeat for this long → `DEAD`        |
| `GOSSIP_INTERVAL_MS`    | `1000`        | How often a node sends heartbeats          |
| `GOSSIP_FANOUT`         | `3`           | Number of random peers gossiped to per round |
| `TTL_SWEEP_INTERVAL_MS` | `5000`        | Background TTL sweep interval              |
| `EVICTION_POLICY`       | `none`        | `none` \| `lru` \| `lfu`                   |
| `EVICTION_THRESHOLD_MB` | `256`         | Memory threshold that triggers eviction    |
| `MAX_RETRIES`           | `1`           | Client retries on a failed request         |
| `RETRY_DELAY_MS`        | `50`          | Delay before a retry                       |
| `LOG_LEVEL`             | `info`        | `debug` \| `info` \| `warn` \| `error`     |

### Run a single node

```bash
npm run dev:node
```

### Run a 3-node cluster locally

```bash
npm run start:cluster
```

This spawns `node-1` (seed, port 3001), `node-2` (port 3002), and `node-3`
(port 3003) concurrently, each with its own HTTP + gossip port.

### Talk to the cluster

```bash
# Set a key (with optional TTL in ms)
curl -X POST http://localhost:3001/set \
  -H "Content-Type: application/json" \
  -d '{"key":"user:42","value":"hello","ttl":60000}'

# Get a key
curl http://localhost:3001/get/user:42

# Delete a key
curl -X DELETE http://localhost:3001/del/user:42

# Node health
curl http://localhost:3001/health
```

Internal node-to-node routes (`/internal/replicate`, `/internal/import-keys`)
are used by the cluster itself for replication and rebalancing — not intended
for direct client use.

### Run tests

```bash
npm test              # run once
npm run test:watch    # watch mode
npm run test:coverage # with coverage report
```

### Load test / failure demo

```bash
npm run start:cluster   # in one terminal
npm run load-test       # in another — drives concurrent SET/GET traffic
```

Then kill one of the node processes mid-test (e.g. `kill -9 <pid>`, or
Task Manager on Windows) and confirm the load test reports zero failed
requests — this is the project's core proof-of-concept.

## Core requirements (in progress)

- [x] Multi-node cluster — independent processes forming one logical cache
- [x] Consistent hashing with virtual nodes (not modulo hashing)
- [x] Replication with configurable replication factor
- [x] Automatic failure detection via gossip (`ALIVE` → `SUSPECT` → `DEAD`)
- [x] Zero-downtime rebalancing on node join/rejoin
- [x] TTL expiry honored consistently across replicas
- [ ] Survive a live node kill mid-traffic with zero failed requests

### Stretch goals

- [ ] LRU/LFU eviction under memory pressure
- [ ] Quorum reads/writes (`X-Consistency: strong | eventual`)

## Design references

This project follows ideas from the papers that underpin real distributed
caches:

- **Dynamo** (DeCandia et al., 2007) — highly-available key-value store design
- **Chord** (Stoica et al.) — consistent hashing ring
- **SWIM** — scalable, infection-style gossip membership protocol
- [Redis Cluster Spec](https://redis.io/docs/reference/cluster-spec) — real-world sharding/replication reference
