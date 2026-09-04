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
  const server = new NodeServer(
    entry.nodeId,
    entry.port,
    store,
    ring,
    new ReplicationManager(entry.nodeId, logger),
    RF,
    membership,
    new KeyMigrator(store, entry.nodeId, logger),
    new NoopEviction(),
    logger
  );
  return { entry, store, ring, membership, server };
}

function requestJson(
  port: number,
  method: string,
  reqPath: string,
  body?: unknown
): Promise<{ status: number; json: any }> {
  const payload = body !== undefined ? JSON.stringify(body) : undefined;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path: reqPath,
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

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe("cluster routing (4 nodes, consistent ring view)", () => {
  // 4 nodes with RF=2 (rather than 3) so that killing one owner still leaves
  // a genuine bystander behind — with exactly RF+1 nodes, removing one owner
  // would trivially promote every survivor to "owner," masking the interesting
  // case: a request landing on a bystander that must be forwarded.
  const nodes = [
    makeNode(makeEntry("node-a", 18091)),
    makeNode(makeEntry("node-b", 18092)),
    makeNode(makeEntry("node-c", 18093)),
    makeNode(makeEntry("node-d", 18094)),
  ];

  beforeAll(async () => {
    // Simulate full gossip convergence: every node's ring/membership already
    // agrees on the full cluster, exactly like it would once gossip settles.
    for (const n of nodes) {
      for (const other of nodes) {
        n.membership.add(other.entry);
        n.ring.addNode(other.entry);
      }
    }
    await Promise.all(nodes.map((n) => n.server.start()));
  });

  afterAll(async () => {
    await Promise.all(nodes.map((n) => n.server.stop()));
  });

  it("SET landing on a non-owner node is forwarded to the real owner, and reads are correct from every node", async () => {
    const key = "routing-key-1";

    // Deliberately hit node-b regardless of who actually owns this key.
    const setRes = await requestJson(nodes[1].entry.port, "POST", "/set", { key, value: "hello" });
    expect(setRes.status).toBe(200);

    const owners = nodes[0].ring.getReplicaNodes(key, RF);
    const ownerIds = new Set(owners.map((o) => o.nodeId));

    await waitFor(() =>
      owners.every((o) => nodes.find((n) => n.entry.nodeId === o.nodeId)!.store.get(key) !== null)
    );

    // Every non-owner's local store must still be empty — the fix's whole point
    // is that only legitimate owners ever hold a copy, not whichever node a
    // client happened to hit.
    for (const n of nodes) {
      if (!ownerIds.has(n.entry.nodeId)) {
        expect(n.store.get(key)).toBeNull();
      }
    }

    for (const n of nodes) {
      const getRes = await requestJson(n.entry.port, "GET", `/get/${key}`);
      expect(getRes.status).toBe(200);
      expect(getRes.json.value).toBe("hello");
    }
  });

  it("reads still succeed after the primary owner is removed from the ring (replica failover)", async () => {
    const key = "routing-key-2";
    await requestJson(nodes[0].entry.port, "POST", "/set", { key, value: "still-here" });

    const [primary, replica] = nodes[0].ring.getReplicaNodes(key, RF);
    const replicaNode = nodes.find((n) => n.entry.nodeId === replica.nodeId)!;
    await waitFor(() => replicaNode.store.get(key) !== null);

    // Simulate the primary dying: survivors drop it from their ring, exactly
    // like CacheNode's node:dead handler does.
    const survivors = nodes.filter((n) => n.entry.nodeId !== primary.nodeId);
    for (const n of survivors) n.ring.removeNode(primary.nodeId);

    // The ring now assigns a *new* second owner to replace the dead primary —
    // that node becomes a nominal owner but was never actually replicated to
    // (re-replicating to restore full RF after a death isn't implemented; see
    // the plan's deferred items). What must hold is: the data isn't lost (the
    // original replica still has it), and every node that ISN'T that dataless
    // new nominal owner — whether it's the replica itself or a plain bystander
    // that has to forward — still serves the correct value.
    const newOwnerIds = new Set(survivors[0].ring.getReplicaNodes(key, RF).map((o) => o.nodeId));
    const dataless = survivors.find(
      (n) => newOwnerIds.has(n.entry.nodeId) && n.store.get(key) === null
    );

    for (const n of survivors) {
      if (dataless && n.entry.nodeId === dataless.entry.nodeId) continue;
      const getRes = await requestJson(n.entry.port, "GET", `/get/${key}`);
      expect(getRes.status).toBe(200);
      expect(getRes.json.value).toBe("still-here");
    }
  });
});
