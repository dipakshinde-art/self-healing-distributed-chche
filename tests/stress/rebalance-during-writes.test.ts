import http from "http";
import { CacheStore } from "../../src/storage/CacheStore";
import { HashRing } from "../../src/hashing/HashRing";
import { MembershipList } from "../../src/gossip/MembershipList";
import { ReplicationManager } from "../../src/replication/ReplicationManager";
import { KeyMigrator } from "../../src/replication/KeyMigrator";
import { NoopEviction } from "../../src/storage/EvictionPolicy";
import { NodeServer } from "../../src/node/NodeServer";
import { createLogger } from "../../src/utils/logger";
import { MembershipEntry } from "../../src/types";

const logger = createLogger("test", "silent");
const RF = 2;

function makeEntry(nodeId: string, port: number): MembershipEntry {
  return { nodeId, host: "127.0.0.1", port, gossipPort: port + 1000, status: "ALIVE", term: 0, lastSeen: Date.now() };
}

function makeNode(entry: MembershipEntry) {
  const store = new CacheStore();
  const ring = new HashRing(150);
  const membership = new MembershipList();
  const migrator = new KeyMigrator(store, entry.nodeId, logger);
  const server = new NodeServer(
    entry.nodeId,
    entry.port,
    store,
    ring,
    new ReplicationManager(entry.nodeId, logger),
    RF,
    membership,
    migrator,
    new NoopEviction(),
    logger
  );
  return { entry, store, ring, membership, migrator, server };
}

type Node = ReturnType<typeof makeNode>;

function requestJson(
  port: number,
  method: string,
  path: string,
  body?: unknown
): Promise<{ status: number; json: any }> {
  const payload = body !== undefined ? JSON.stringify(body) : undefined;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path,
        method,
        headers: payload
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) }
          : undefined,
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, json: data ? JSON.parse(data) : null }));
      }
    );
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 20));
  }
}

// Mirrors CacheNode's gossip-driven ring sync + migration hookup, applied
// manually here since this test drives NodeServer directly rather than
// through real gossip timing.
function syncRingAndMigrate(node: Node, newEntry: MembershipEntry): void {
  node.membership.add(newEntry);
  node.ring.addNode(newEntry);
  const keysForNew = node.ring.getKeysForNode(node.store.keys(), newEntry.nodeId);
  if (keysForNew.length > 0) {
    node.migrator.migrateKeys(keysForNew, newEntry).catch(() => {});
  }
}

describe("stress: rebalancing during continuous writes", () => {
  it("loses no data when a new node joins mid-traffic", async () => {
    const a = makeNode(makeEntry("stress-a", 18201));
    const b = makeNode(makeEntry("stress-b", 18202));
    const c = makeNode(makeEntry("stress-c", 18203));
    const d = makeNode(makeEntry("stress-d", 18204));
    const initial = [a, b, c];

    for (const n of initial) {
      for (const other of initial) {
        n.membership.add(other.entry);
        n.ring.addNode(other.entry);
      }
    }

    await Promise.all([...initial, d].map((n) => n.server.start()));

    try {
      const written = new Map<string, string>();
      const TOTAL_KEYS = 300;
      const JOIN_AT = 100;

      for (let i = 0; i < TOTAL_KEYS; i++) {
        if (i === JOIN_AT) {
          // A admits D through the real join handshake; B, C, and D itself
          // converge their ring/membership the way gossip would in production
          // (CacheNode's membership:changed handler / joinSeedNode).
          const joinRes = await requestJson(a.entry.port, "POST", "/internal/join", { entry: d.entry });
          for (const member of joinRes.json.members as MembershipEntry[]) {
            d.membership.add(member);
            d.ring.addNode(member);
          }
          syncRingAndMigrate(b, d.entry);
          syncRingAndMigrate(c, d.entry);
        }

        const key = `stress-key-${i}`;
        const value = `value-${i}`;
        const node = initial[i % initial.length];
        const res = await requestJson(node.entry.port, "POST", "/set", { key, value });
        expect(res.status).toBe(200);
        written.set(key, value);
      }

      // Let any in-flight replication/migration settle.
      await waitFor(() => {
        const total = a.store.size() + b.store.size() + c.store.size() + d.store.size();
        return total >= written.size; // replicated copies mean this can exceed written.size
      });
      await new Promise((r) => setTimeout(r, 300));

      for (const [key, expected] of written) {
        const queryNode = [a, b, c, d][Math.floor(Math.random() * 4)];
        const res = await requestJson(queryNode.entry.port, "GET", `/get/${key}`);
        expect(res.status).toBe(200);
        expect(res.json.value).toBe(expected);
      }
    } finally {
      await Promise.all([a, b, c, d].map((n) => n.server.stop()));
    }
  }, 30000);
});
