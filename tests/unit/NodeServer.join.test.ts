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

function makeEntry(nodeId: string, port: number): MembershipEntry {
  return { nodeId, host: "127.0.0.1", port, gossipPort: port + 1000, status: "ALIVE", term: 0, lastSeen: Date.now() };
}

function makeNode(nodeId: string, port: number) {
  const store = new CacheStore();
  const ring = new HashRing(150);
  const membership = new MembershipList();
  const self = makeEntry(nodeId, port);
  membership.add(self);
  ring.addNode(self);

  const server = new NodeServer(
    nodeId,
    port,
    store,
    ring,
    new ReplicationManager(nodeId, logger),
    2,
    membership,
    new KeyMigrator(store, nodeId, logger),
    new NoopEviction(),
    logger
  );

  return { store, ring, membership, server, self };
}

function postJson(port: number, path: string, body: unknown): Promise<{ status: number; json: any }> {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) },
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

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe("NodeServer /internal/join", () => {
  it("admits a joining node into membership + ring and returns the full membership list", async () => {
    const seed = makeNode("seed-1", 18081);
    const joiner = makeEntry("joiner-1", 18082);
    await seed.server.start();

    try {
      const res = await postJson(18081, "/internal/join", { entry: joiner });

      expect(res.status).toBe(200);
      const memberIds = res.json.members.map((m: MembershipEntry) => m.nodeId);
      expect(memberIds).toEqual(expect.arrayContaining(["seed-1", "joiner-1"]));

      expect(seed.membership.get("joiner-1")).toBeDefined();
      expect(seed.ring.getNodeCount()).toBe(2);
    } finally {
      await seed.server.stop();
    }
  });

  it("migrates keys the new node now owns, so the joiner isn't empty for its share of the ring", async () => {
    const seed = makeNode("seed-2", 18083);
    const joiner = makeNode("joiner-2", 18084);

    for (let i = 0; i < 200; i++) {
      seed.store.set(`key-${i}`, `value-${i}`);
    }

    await seed.server.start();
    await joiner.server.start();

    try {
      await postJson(18083, "/internal/join", { entry: joiner.self });

      // Compute which keys the seed's ring now assigns to the joiner — that's exactly
      // what KeyMigrator should have streamed over asynchronously after the join response.
      const expectedMigrated = seed.ring.getKeysForNode(
        Array.from({ length: 200 }, (_, i) => `key-${i}`),
        "joiner-2"
      );
      expect(expectedMigrated.length).toBeGreaterThan(0);

      await waitFor(() => joiner.store.size() >= expectedMigrated.length);

      for (const key of expectedMigrated) {
        expect(joiner.store.get(key)).not.toBeNull();
        expect(seed.store.get(key)).toBeNull(); // migrated away from the source
      }
    } finally {
      await seed.server.stop();
      await joiner.server.stop();
    }
  });
});
