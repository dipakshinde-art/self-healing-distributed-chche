import { formatStatusTable, buildSpawnConfig } from "../../src/cli/cli";
import { MembershipEntry } from "../../src/types";

function makeEntry(nodeId: string): MembershipEntry {
  return { nodeId, host: "127.0.0.1", port: 3001, gossipPort: 4001, status: "ALIVE", term: 3, lastSeen: Date.now() };
}

describe("formatStatusTable", () => {
  it("includes the node id, key count, and every member row", () => {
    const table = formatStatusTable("node-1", 42, [makeEntry("node-1"), makeEntry("node-2")]);

    expect(table).toContain("Node: node-1");
    expect(table).toContain("Keys: 42");
    expect(table).toContain("node-1");
    expect(table).toContain("node-2");
    expect(table).toContain("ALIVE");
    expect(table).toContain("term=3");
  });

  it("renders an empty membership list without throwing", () => {
    expect(() => formatStatusTable("node-1", 0, [])).not.toThrow();
  });
});

describe("buildSpawnConfig", () => {
  it("coerces string CLI args into a typed NodeSpawnConfig", () => {
    const config = buildSpawnConfig({
      nodeId: "node-4",
      port: "3004",
      gossipPort: "4004",
      seedHost: "127.0.0.1",
      seedPort: "3001",
    });

    expect(config).toEqual({
      nodeId: "node-4",
      host: "127.0.0.1",
      port: 3004,
      gossipPort: 4004,
      seedHost: "127.0.0.1",
      seedPort: 3001,
    });
  });

  it("respects an explicit host override", () => {
    const config = buildSpawnConfig({
      nodeId: "node-5",
      port: "3005",
      gossipPort: "4005",
      seedHost: "127.0.0.1",
      seedPort: "3001",
      host: "10.0.0.5",
    });

    expect(config.host).toBe("10.0.0.5");
  });
});
