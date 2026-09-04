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
const RF = 3;

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

function postJson(
  port: number,
  path: string,
  body: unknown,
  headers: Record<string, string> = {}
): Promise<{ status: number; json: any }> {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload), ...headers },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, json: JSON.parse(data) }));
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

describe("quorum writes (X-Consistency: strong)", () => {
  // RF=3 so quorum = floor(3/2)+1 = 2 — the local write plus at least one replica ack.
  const nodes = [
    makeNode(makeEntry("q-a", 18101)),
    makeNode(makeEntry("q-b", 18102)),
    makeNode(makeEntry("q-c", 18103)),
  ];

  beforeAll(() => {
    for (const n of nodes) {
      for (const other of nodes) {
        n.membership.add(other.entry);
        n.ring.addNode(other.entry);
      }
    }
  });

  it("eventual mode (default) returns success without a consistency/acked field", async () => {
    await Promise.all(nodes.map((n) => n.server.start()));
    try {
      const owner = nodes.find((n) => n.ring.getPrimaryNode("evk1")!.nodeId === n.entry.nodeId)!;
      const res = await postJson(owner.entry.port, "/set", { key: "evk1", value: "v1" });
      expect(res.status).toBe(200);
      expect(res.json).toEqual({ ok: true, version: expect.any(Number) });
    } finally {
      await Promise.all(nodes.map((n) => n.server.stop()));
    }
  });

  it("strong mode succeeds once a quorum of replicas ack", async () => {
    await Promise.all(nodes.map((n) => n.server.start()));
    try {
      const owner = nodes.find((n) => n.ring.getPrimaryNode("strongkey")!.nodeId === n.entry.nodeId)!;
      const res = await postJson(
        owner.entry.port,
        "/set",
        { key: "strongkey", value: "v-strong" },
        { "X-Consistency": "strong" }
      );
      expect(res.status).toBe(200);
      expect(res.json).toMatchObject({ ok: true, consistency: "strong", acked: 3 });
    } finally {
      await Promise.all(nodes.map((n) => n.server.stop()));
    }
  });

  it("strong mode fails with 503 if quorum can't be reached", async () => {
    const owner = nodes.find((n) => n.ring.getPrimaryNode("failkey")!.nodeId === n.entry.nodeId)!;

    // Start only the owner — both replicas are unreachable, so replication
    // can't ack and quorum (2) can't be met beyond the owner's own local write.
    await owner.server.start();
    try {
      const res = await postJson(
        owner.entry.port,
        "/set",
        { key: "failkey", value: "v-fail" },
        { "X-Consistency": "strong" }
      );
      expect(res.status).toBe(503);
      expect(res.json).toMatchObject({ ok: false, acked: 1, required: 2 });
    } finally {
      await owner.server.stop();
    }
  });
});
