import { MembershipList } from "../../src/gossip/MembershipList";
import { NodeStateManager } from "../../src/gossip/NodeStateManager";
import { createLogger } from "../../src/utils/logger";
import { MembershipEntry } from "../../src/types";

const logger = createLogger("test", "silent");

function makeEntry(nodeId: string): MembershipEntry {
  return { nodeId, host: "127.0.0.1", port: 3001, gossipPort: 4001, status: "ALIVE", term: 0, lastSeen: Date.now() };
}

describe("NodeStateManager", () => {
  const SUSPECT_TIMEOUT_MS = 3000;
  const DEAD_TIMEOUT_MS = 6000;

  afterEach(() => {
    jest.useRealTimers();
  });

  it("marks a node SUSPECT on a missed heartbeat, then DEAD after the dead timeout elapses", () => {
    jest.useFakeTimers();
    const membership = new MembershipList();
    membership.add(makeEntry("peer-1"));
    const sm = new NodeStateManager(membership, SUSPECT_TIMEOUT_MS, DEAD_TIMEOUT_MS, logger);

    const suspectSpy = jest.fn();
    const deadSpy = jest.fn();
    sm.on("node:suspect", suspectSpy);
    sm.on("node:dead", deadSpy);

    sm.missedHeartbeat("peer-1");

    expect(suspectSpy).toHaveBeenCalledWith("peer-1");
    expect(membership.get("peer-1")?.status).toBe("SUSPECT");
    expect(deadSpy).not.toHaveBeenCalled();

    jest.advanceTimersByTime(DEAD_TIMEOUT_MS - SUSPECT_TIMEOUT_MS);

    expect(deadSpy).toHaveBeenCalledWith("peer-1");
    expect(membership.get("peer-1")?.status).toBe("DEAD");

    sm.destroy();
  });

  it("a heartbeat received while SUSPECT cancels the dead timer and flips back to ALIVE", () => {
    jest.useFakeTimers();
    const membership = new MembershipList();
    membership.add(makeEntry("peer-2"));
    const sm = new NodeStateManager(membership, SUSPECT_TIMEOUT_MS, DEAD_TIMEOUT_MS, logger);

    const aliveSpy = jest.fn();
    const deadSpy = jest.fn();
    sm.on("node:alive", aliveSpy);
    sm.on("node:dead", deadSpy);

    sm.missedHeartbeat("peer-2");
    expect(membership.get("peer-2")?.status).toBe("SUSPECT");

    sm.heartbeatReceived("peer-2", 1);
    expect(aliveSpy).toHaveBeenCalledWith("peer-2");
    expect(membership.get("peer-2")?.status).toBe("ALIVE");

    // The dead timer that was scheduled on missedHeartbeat must have been
    // cleared — otherwise the node would incorrectly flip to DEAD later.
    jest.advanceTimersByTime(DEAD_TIMEOUT_MS);
    expect(deadSpy).not.toHaveBeenCalled();

    sm.destroy();
  });

  it("ignores a missed heartbeat for a node that isn't currently ALIVE", () => {
    jest.useFakeTimers();
    const membership = new MembershipList();
    membership.add(makeEntry("peer-3"));
    const sm = new NodeStateManager(membership, SUSPECT_TIMEOUT_MS, DEAD_TIMEOUT_MS, logger);

    sm.missedHeartbeat("peer-3"); // ALIVE -> SUSPECT
    const suspectSpy = jest.fn();
    sm.on("node:suspect", suspectSpy);

    sm.missedHeartbeat("peer-3"); // already SUSPECT, should be a no-op
    expect(suspectSpy).not.toHaveBeenCalled();

    sm.destroy();
  });
});
