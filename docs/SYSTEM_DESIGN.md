# Self-Healing Distributed Cache — System Design Scenario

A from-scratch, Redis-shaped key-value cache: consistent hashing for placement,
gossip for failure detection, async replication for durability, and streamed
rebalancing for zero-downtime membership changes. This walks through the
scenario the system is built to survive, component by component, request by
request.

**Cluster used throughout this doc:** 3 nodes, replication factor 2.

---

## §1 The scenario

The brief, stated the way it would be in a system-design interview: *build a
cache that clients can write to and read from through any of several
machines, where killing one machine mid-traffic causes zero failed client
requests* — no leader election, no external coordinator, no Redis underneath.

Three mechanisms have to work together for that to hold:

1. **Placement** — every node must agree, without asking a coordinator, which
   node(s) own a given key.
2. **Detection** — a dead node has to be noticed by the survivors within
   seconds, not minutes.
3. **Continuity** — data for a key has to already exist somewhere else before
   its owner disappears, and has to move automatically when the membership
   changes.

Requirements tracked by the project, with status as of this pass:

| | Requirement | Status |
|---|---|---|
| ✅ | Multi-node cluster — independent processes forming one logical cache | **fixed this pass** |
| ✅ | Consistent hashing with virtual nodes (not modulo hashing) | verified |
| ✅ | Replication with configurable replication factor | verified |
| ✅ | Automatic failure detection via gossip (`ALIVE` → `SUSPECT` → `DEAD`) | verified |
| ✅ | Zero-downtime rebalancing on node join/rejoin | **fixed this pass** |
| ✅ | TTL expiry honored consistently across replicas | verified |
| ⬜ | Survive a live node kill mid-traffic with zero failed requests | depends on all of the above — see §3.4; not yet load-tested |

---

## §1a Try it yourself

This is the exact sequence used to produce every log snippet in this doc.
Three terminals, one cluster:

```bash
npm run start:cluster          # boots node-1 (seed, :3001), node-2 (:3002), node-3 (:3003)
```

Write 20 keys through node-1 — note that you never have to tell it which
node "owns" `k1`..`k20`; that's the whole point:

```bash
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:3001/set \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"k$i\",\"value\":\"v$i\"}"
done
```

Check how the 20 keys actually landed — captured from a real run:

```bash
$ curl -s http://localhost:3001/health
{"nodeId":"node-1","status":"ALIVE","keys":20}
$ curl -s http://localhost:3002/health
{"nodeId":"node-2","status":"ALIVE","keys":15}
$ curl -s http://localhost:3003/health
{"nodeId":"node-3","status":"ALIVE","keys":8}
```

node-1 has all 20 because it received every write directly (§3.2 explains
why); node-2 and node-3 have most of them too, because they're each some
key's replica at RF=2. The overlap is expected — with 3 nodes and RF=2,
every key lives on exactly 2 of them.

Read one back from node-1 — same request, real response:

```bash
$ curl -s http://localhost:3001/get/k5
{"key":"k5","value":"v5","version":5}
```

---

## §2 Topology

Every physical node projects onto the ring as 150 virtual nodes, so ownership
spreads evenly and losing one machine reshuffles roughly `1/N` of the
keyspace instead of an unpredictable chunk.

```
                     node-1 (seed, :3001)
                            ●
                     ╱             ╲
                   ╱                 ╲
                 ╱      hash ring       ╲
               ╱      (150 vnodes         ╲
              │        per node)           │
                ╲                        ╱
                  ╲                    ╱
                    ╲                ╱
                       ●          ●
              node-3 (:3003)  node-2 (:3002)
```

`HashRing.getPrimaryNode(key)` walks clockwise from `hash(key)` to the first
vnode it finds — that vnode's owner is the primary. `getReplicaNodes(key, RF)`
keeps walking clockwise, skipping duplicates, until it has collected `RF`
distinct physical nodes.

**Worked example.** Real output from feeding three keys through this exact
3-node ring (`HashRing.getPrimaryNode` / `getReplicaNodes`, MurmurHash3 via
`src/hashing/murmur.ts`):

```
key            hash          primary    replicas (RF=2)
user:42        1288969721    node-1     [node-1, node-2]
k5             2900501609    node-2     [node-2, node-1]
session:abc    1418561701    node-2     [node-2, node-3]
```

Two things worth noticing: `hash()` doesn't care what the key *means* —
`user:42` and `k5` are just byte strings to MurmurHash3, and their hashes
land in totally different, unrelated places on the ring. And "replicas"
always includes the primary itself as the first entry — `getReplicaNodes`
is "the primary, plus RF−1 more," not "RF nodes other than the primary."
That's why `NodeServer.handleSet` has to explicitly filter out `this.nodeId`
before replicating (§3.2) — otherwise a node would try to replicate to
itself.

### Components on each node

| Component | File | Job |
|---|---|---|
| HashRing | `src/hashing/HashRing.ts` | Owns the vnode ring; answers "who owns this key" and "who are its N replicas." |
| CacheStore | `src/storage/CacheStore.ts` | The actual `Map<key, entry>`. Versioned entries, last-write-wins on import. |
| MembershipList | `src/gossip/MembershipList.ts` | This node's view of every node's status + term. Merges incoming gossip by higher term. |
| GossipAgent | `src/gossip/GossipAgent.ts` | UDP heartbeat every 1s to a random fanout of peers; carries the full membership list. |
| NodeStateManager | `src/gossip/NodeStateManager.ts` | State machine: missed heartbeats → SUSPECT → (timeout) → DEAD. Emits the events everything else reacts to. |
| ReplicationManager | `src/replication/ReplicationManager.ts` | Fire-and-forget HTTP push of a write to its replica set. |
| KeyMigrator | `src/replication/KeyMigrator.ts` | Batches and streams keys to a node that just joined or recovered, then deletes them locally. |
| NodeServer | `src/node/NodeServer.ts` | The HTTP surface — client routes (`/get`/`/set`/`/del`) and internal routes (`/internal/replicate`, `/internal/import-keys`, `/internal/join`). |
| EvictionPolicy | `src/storage/EvictionPolicy.ts` | LRU/LFU candidate ranking, fed live by every get/set/delete that passes through NodeServer. |

---

## §3 Request flows

Six scenarios cover everything the cluster does. The first and last are the
two this pass took from stub to working.

### 3.1 — Cluster bootstrap · **fixed this pass**

*node-2 joins via seed node-1* — `src/node/CacheNode.ts`, `src/node/NodeServer.ts`

1. node-2 registers itself as its own sole member, then — since its
   `SEED_HOST:SEED_PORT` doesn't match its own address — calls
   `joinSeedNode()`.
2. It POSTs its own `MembershipEntry` to `node-1:3001/internal/join`, retrying
   up to 20× on a 500ms backoff — the seed may still be starting up, since
   `start:cluster` launches all three processes at once. (`withRetry` here
   uses its own constants, separate from the client-facing `MAX_RETRIES`
   config, which is tuned for a fast op, not "wait for a process to boot.")
3. node-1's `handleJoin` adds node-2 to its membership list and ring, hands
   back its *entire* membership list in one response, then — because node-2
   is new — computes which of its own keys the ring now assigns to node-2
   and streams them over via `KeyMigrator`.
4. node-2 adopts every peer in the response into its own membership + ring.
   From this point its gossip agent has real peers to talk to.

Captured from an actual local run of `npm run start:cluster` after the fix —
node-1's log, unedited:

```
{"level":30,"nodeId":"node-1","msg":"Starting cache node","port":3001}
{"level":30,"nodeId":"node-1","msg":"Gossip agent listening","port":4001}
{"level":30,"nodeId":"node-1","msg":"Node HTTP server listening","port":3001}
{"level":30,"nodeId":"node-1","msg":"Cache node fully started"}
{"level":30,"nodeId":"node-1","msg":"Node joined cluster","nodeId":"node-3"}
{"level":30,"nodeId":"node-1","msg":"Node joined cluster","nodeId":"node-2"}
```

Before the fix, `joinSeedNode()` was a logging no-op — every node's
membership list only ever contained itself, so these two lines never
appeared and the ring never grew past one node.

**Example — the actual bytes on the wire.** This is what node-2 POSTs and
what node-1 hands back, shape-for-shape (`JoinRequest`/`JoinResponse` in
`src/types/index.ts`):

```jsonc
// POST http://127.0.0.1:3001/internal/join   (node-2 → node-1)
{
  "entry": {
    "nodeId": "node-2", "host": "127.0.0.1", "port": 3002,
    "gossipPort": 4002, "status": "ALIVE", "term": 0, "lastSeen": 1735...
  }
}

// 200 response body (node-1 → node-2)
{
  "members": [
    { "nodeId": "node-1", "port": 3001, "status": "ALIVE", ... },
    { "nodeId": "node-2", "port": 3002, "status": "ALIVE", ... }
  ]
}
```

node-2 never has to ask again — one response gives it every peer node-1
currently knows about, which is why the join is a single round trip instead
of a crawl.

### 3.2 — Write path

*SET user:42 → "hello"* — `src/node/NodeServer.ts:handleSet`

1. A client POSTs to *any* node's `/set` — there's no forwarding to a
   "primary." The receiving node writes to its own `CacheStore` immediately
   and returns.
2. It records the write with its eviction tracker (`recordSet`), then
   computes `ring.getReplicaNodes(key, RF)` and fires an async,
   fire-and-forget replicate call to each replica that isn't itself.
3. The client gets its response before replication finishes — this is an
   eventually-consistent write, traded deliberately for latency (see §4).

**Example:**

```bash
$ curl -X POST http://localhost:3001/set \
    -H "Content-Type: application/json" \
    -d '{"key":"k5","value":"v5"}'
{"ok":true,"version":5}
```

`version` is `CacheStore`'s own monotonic counter (not a per-key version —
every write on a node, any key, increments the same counter). It's what
`importEntry` compares on replication/migration to decide last-write-wins.

### 3.3 — Read path

*GET user:42* — `src/node/NodeServer.ts:handleGet`

1. Read hits whichever node the client asked — a hit returns the entry (with
   its version) and records the access for LRU/LFU tracking; a miss (wrong
   node, or truly absent) returns 404.
2. There's no read-repair or quorum read yet — a client that always talks to
   the same node sees whatever that node's copy holds, which can lag the
   primary by one async replication hop.

**Example — same key, read from node-1 directly:**

```bash
$ curl -s http://localhost:3001/get/k5
{"key":"k5","value":"v5","version":5}
$ curl -s http://localhost:3001/get/does-not-exist
{"error":"Key not found"}
```

### 3.4 — Failure detection & self-healing

*node-3 process is killed mid-traffic* — `src/gossip/NodeStateManager.ts`, `src/gossip/GossipAgent.ts`

1. Every peer's gossip tick checks `now − member.lastSeen > 2×interval`. Once
   node-3 stops sending/relaying heartbeats, its peers stop refreshing
   `lastSeen` for it.
2. At `SUSPECT_TIMEOUT_MS` (default 3000ms), `missedHeartbeat()` flips it to
   **SUSPECT** and starts a dead-timer.
3. At `DEAD_TIMEOUT_MS` (default 6000ms) with no recovery, it flips to
   **DEAD** and emits `node:dead` — `CacheNode.ts` reacts by pulling it out
   of the hash ring entirely.
4. Keys node-3 used to own now resolve to the next node clockwise on the
   ring — which, at RF≥2, already holds a replica. Reads and writes for
   those keys keep succeeding without any client-visible error.

Captured from the same run, ~45 seconds after `taskkill /F` on node-3's
process:

```
{"level":30,"nodeId":"node-1","msg":"Node joined cluster","nodeId":"node-3"}
{"level":30,"nodeId":"node-1","msg":"Node joined cluster","nodeId":"node-2"}
{"level":40,"nodeId":"node-1","msg":"Node marked SUSPECT","nodeId":"node-3"}
{"level":50,"nodeId":"node-1","msg":"Node marked DEAD","nodeId":"node-3"}
{"level":50,"nodeId":"node-1","msg":"Removing dead node from ring","nodeId":"node-3"}
```

Gap between SUSPECT and DEAD is ~3s, matching
`SUSPECT_TIMEOUT_MS`→`DEAD_TIMEOUT_MS` — this is the load-bearing timing
behind the README's "kill a node, zero failed requests" claim.

### 3.5 — Recovery / rejoin

*node-3 process is restarted* — `src/node/CacheNode.ts`, `src/replication/KeyMigrator.ts`

1. node-3 boots fresh and re-runs the §3.1 join flow against the seed — it
   re-enters the cluster as a normal join, not a special "rejoin" path.
2. On the peers' side, if node-3's heartbeat resumes *before* it was marked
   DEAD, `heartbeatReceived()` flips it straight back to **ALIVE** and
   `node:alive` fires — CacheNode re-adds it to the ring and migrates back
   the keys it now owns.
3. Either path ends the same way: the ring has 3 nodes again, and whichever
   node held node-3's share of keys in the meantime streams them back via
   `KeyMigrator`.

### 3.6 — TTL expiry

*a key set with a TTL outlives its window* — `src/ttl/TTLEngine.ts`, `src/storage/CacheStore.ts`

1. Every read checks `expiresAt` and deletes-then-returns-null on the spot
   (lazy eviction) — a stale key never has to wait for the sweep to become
   invisible to readers.
2. A background sweep (`TTL_SWEEP_INTERVAL_MS`, default 5s) also runs
   `purgeExpired()` so cold keys that nobody reads still get reclaimed.
3. Expiry isn't itself gossiped — each replica expires the same key
   independently once its own clock crosses `expiresAt`, which is set at
   write time and copied verbatim on replication.

---

## §4 Design decisions

**Gossip over a coordinator.** No node is ever asked "is node-3 alive" —
every node forms its own opinion from heartbeats and propagates it. This is
what makes the cluster survive without a leader-election story, at the cost
of every node's view being slightly stale and occasionally briefly wrong (a
slow node can look SUSPECT for a few seconds even though it's fine).
*Trade-off: no single point of failure for detection ↔ every node's
world-view is approximate.*

**Write-through-any-node, not primary-forwarded.** `handleSet` always writes
locally on whichever node received the request, then replicates to the
ring's computed owners — it never proxies the write to the "real" primary
first. Simpler and lower-latency, but it means a client hitting a
non-owning node creates a copy that isn't one of the ring's official replica
slots until the next write reconciles it.
*Trade-off: one hop instead of two ↔ replica set isn't strictly
authoritative.*

**Last-write-wins by version, not vector clocks.** `CacheStore.importEntry`
keeps whichever copy has the higher monotonic version counter. Cheap and
deterministic, but two concurrent writes to the same key from different
nodes aren't merged — one is silently dropped, exactly the trade-off Dynamo
made vector clocks to avoid.
*Trade-off: O(1) conflict resolution ↔ no causal history, so silent
last-writer-wins.*

**Fire-and-forget replication.** The client gets its response before
replicas confirm. Fast, and matches the "don't block on the network" ethos
of the whole project — but a crash between the local write and the replica
ack is a real, if narrow, durability gap.
*Trade-off: low write latency ↔ eventual, not synchronous, consistency.*

**Eviction tracked at the request layer, not inside CacheStore.**
`LRUEviction`/`LFUEviction` only rank candidates against whatever
`store.keys()` currently is — they hold no cached count. `NodeServer` feeds
them via `recordAccess`/`recordSet`/`recordDelete` on every request,
mirroring how `TTLEngine` already sits outside `CacheStore` and drives it
from the outside. This keeps `CacheStore` a pure data structure and makes
eviction candidates immune to drift — an LRU/LFU-tracked key that expired
via TTL between accesses is filtered out automatically rather than returned
stale.
*Trade-off: store stays a dependency-free Map wrapper ↔ two call sites to
keep in sync instead of one.*

---

## §5 Open gaps

What the scenario above still doesn't cover, in order of how much it would
change if closed:

- **Seed is a SPOF.** Bootstrap only ever talks to one seed address. If the
  seed is down when every other node starts, the whole cluster fails to
  form — there's no fallback seed list.
- **No quorum reads.** The `X-Consistency: strong | eventual` stretch goal
  isn't implemented; every read is whatever-one-node-has, not a majority
  read across replicas.
- **Eviction untriggered.** `EVICTION_THRESHOLD_MB` is parsed by config but
  nothing measures live memory and calls `getEvictionCandidates()` yet — the
  ranking logic is real and tested, but no sweep invokes it.
- **No auth/TLS.** Every HTTP and UDP path is plaintext, unauthenticated —
  fine for a local cluster, not for a shared network.

---

## §6 Lineage

The README names its references directly; here's what each one actually
shows up as in the code:

| Paper | Shows up as |
|---|---|
| **Dynamo** (2007) | Consistent hashing for placement, replication factor, last-write-wins via a version counter instead of Dynamo's own vector clocks. |
| **Chord** | The ring itself — `HashRing.findClockwiseIndex` is a binary search for "the first vnode at or after this hash," Chord's core lookup. |
| **SWIM** | `GossipAgent`'s random-fanout UDP heartbeats and `NodeStateManager`'s ALIVE→SUSPECT→DEAD state machine, without SWIM's indirect ping step. |

---

*Generated from the source tree at the time of writing. Re-derive rather than
trust blindly if the code has moved on since.*
