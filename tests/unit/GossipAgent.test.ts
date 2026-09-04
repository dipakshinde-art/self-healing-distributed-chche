import { MembershipList } from "../../src/gossip/MembershipList";
import { NodeStateManager } from "../../src/gossip/NodeStateManager";
import { GossipAgent } from "../../src/gossip/GossipAgent";
import { createLogger } from "../../src/utils/logger";
import { MembershipEntry } from "../../src/types";

const logger = createLogger("test", "silent");

function makeEntry(nodeId: string, gossipPort: number): MembershipEntry {
  return {
    nodeId,
    host: "127.0.0.1",
    port: gossipPort - 1000,
    gossipPort,
    status: "ALIVE",
    term: 0,
    lastSeen: Date.now(),
  };
}

function makeAgent(nodeId: string, gossipPort: number, intervalMs = 50) {
  const membership = new MembershipList();
  const self = makeEntry(nodeId, gossipPort);
  membership.add(self);
  const stateManager = new NodeStateManager(membership, 200, 400, logger);
  const agent = new GossipAgent(nodeId, "127.0.0.1", gossipPort, membership, stateManager, 3, intervalMs, logger);
  return { self, membership, stateManager, agent };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe("GossipAgent", () => {
  it("heartbeats merge each peer's membership entry into the other's list", async () => {
    const a = makeAgent("agent-a", 19201);
    const b = makeAgent("agent-b", 19202);

    a.membership.add(b.self);
    b.membership.add(a.self);

    await a.agent.start();
    await b.agent.start();

    try {
      await waitFor(
        () => a.membership.get("agent-b") !== undefined && b.membership.get("agent-a") !== undefined
      );
      expect(a.membership.get("agent-b")?.nodeId).toBe("agent-b");
      expect(b.membership.get("agent-a")?.nodeId).toBe("agent-a");
    } finally {
      a.agent.stop();
      b.agent.stop();
    }
  });

  it("a peer that never sends a heartbeat is eventually marked SUSPECT", async () => {
    const a = makeAgent("agent-c", 19203, 50);
    const ghost = makeEntry("agent-ghost", 19299); // no agent actually listening here
    a.membership.add(ghost);

    const suspectSpy = jest.fn();
    a.stateManager.on("node:suspect", suspectSpy);

    await a.agent.start();
    try {
      await waitFor(() => suspectSpy.mock.calls.length > 0);
      expect(suspectSpy).toHaveBeenCalledWith("agent-ghost");
      expect(a.membership.get("agent-ghost")?.status).toBe("SUSPECT");
    } finally {
      a.agent.stop();
    }
  });
});
