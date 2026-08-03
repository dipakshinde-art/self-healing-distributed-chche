import { HashRing } from "../../src/hashing/HashRing";
import { MembershipEntry } from "../../src/types";

function makeNode(id: string, port: number): MembershipEntry {
  return { nodeId: id, host: "127.0.0.1", port, gossipPort: port + 1000, status: "ALIVE", term: 0, lastSeen: Date.now() };
}

describe("HashRing", () => {
  it("routes all keys to the single node when only one exists", () => {
    const ring = new HashRing(50);
    ring.addNode(makeNode("node-1", 3001));

    for (let i = 0; i < 100; i++) {
      expect(ring.getPrimaryNode(`key-${i}`)?.nodeId).toBe("node-1");
    }
  });

  it("distributes keys roughly evenly across 3 nodes", () => {
    const ring = new HashRing(150);
    ring.addNode(makeNode("node-1", 3001));
    ring.addNode(makeNode("node-2", 3002));
    ring.addNode(makeNode("node-3", 3003));

    const counts: Record<string, number> = { "node-1": 0, "node-2": 0, "node-3": 0 };

    for (let i = 0; i < 3000; i++) {
      const owner = ring.getPrimaryNode(`key-${i}`)?.nodeId;
      if (owner) counts[owner]++;
    }

    // Each node should own roughly 33% ± 10%
    for (const count of Object.values(counts)) {
      expect(count).toBeGreaterThan(700);  // > ~23%
      expect(count).toBeLessThan(1300);    // < ~43%
    }
  });

  it("reshuffles fewer than 40% of keys when adding a 4th node to a 3-node cluster", () => {
    const ring = new HashRing(150);
    ring.addNode(makeNode("node-1", 3001));
    ring.addNode(makeNode("node-2", 3002));
    ring.addNode(makeNode("node-3", 3003));

    const keys = Array.from({ length: 1000 }, (_, i) => `key-${i}`);
    const before = keys.map((k) => ring.getPrimaryNode(k)?.nodeId);

    ring.addNode(makeNode("node-4", 3004));

    const after = keys.map((k) => ring.getPrimaryNode(k)?.nodeId);
    const moved = before.filter((v, i) => v !== after[i]).length;

    // Consistent hashing: only ~K/N keys should move = ~25%
    expect(moved).toBeLessThan(400); // well under 40%
  });

  it("returns RF replica nodes without duplicates", () => {
    const ring = new HashRing(150);
    ring.addNode(makeNode("node-1", 3001));
    ring.addNode(makeNode("node-2", 3002));
    ring.addNode(makeNode("node-3", 3003));

    const replicas = ring.getReplicaNodes("test-key", 3);
    const ids = replicas.map((r) => r.nodeId);

    expect(ids.length).toBe(3);
    expect(new Set(ids).size).toBe(3); // no duplicates
  });

  it("returns null when ring is empty", () => {
    const ring = new HashRing(150);
    expect(ring.getPrimaryNode("some-key")).toBeNull();
  });
});
