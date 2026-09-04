import { EventEmitter } from "events";
import { spawn } from "child_process";
import { ClusterManager } from "../../src/cluster/ClusterManager";
import { createLogger } from "../../src/utils/logger";

jest.mock("child_process", () => ({ spawn: jest.fn() }));

const logger = createLogger("test", "silent");

function fakeChildProcess() {
  const child = new EventEmitter() as EventEmitter & { kill: jest.Mock; stdout: EventEmitter; stderr: EventEmitter };
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = jest.fn();
  return child;
}

describe("ClusterManager", () => {
  beforeEach(() => {
    (spawn as jest.Mock).mockReset();
  });

  it("spawns a node process with the right env vars and tracks it as running", () => {
    const child = fakeChildProcess();
    (spawn as jest.Mock).mockReturnValue(child);

    const manager = new ClusterManager("src/node/CacheNode.ts", logger);
    manager.spawnNode({
      nodeId: "node-x",
      host: "127.0.0.1",
      port: 3010,
      gossipPort: 4010,
      seedHost: "127.0.0.1",
      seedPort: 3001,
    });

    expect(spawn).toHaveBeenCalledTimes(1);
    const [, , options] = (spawn as jest.Mock).mock.calls[0];
    expect(options.env.NODE_ID).toBe("node-x");
    expect(options.env.NODE_PORT).toBe("3010");
    expect(options.env.SEED_PORT).toBe("3001");
    expect(manager.getRunningNodes()).toEqual(["node-x"]);
  });

  it("removes a node from the running set once its process exits", () => {
    const child = fakeChildProcess();
    (spawn as jest.Mock).mockReturnValue(child);

    const manager = new ClusterManager("src/node/CacheNode.ts", logger);
    manager.spawnNode({
      nodeId: "node-y",
      host: "127.0.0.1",
      port: 3011,
      gossipPort: 4011,
      seedHost: "127.0.0.1",
      seedPort: 3001,
    });

    expect(manager.getRunningNodes()).toEqual(["node-y"]);
    child.emit("exit", 1);
    expect(manager.getRunningNodes()).toEqual([]);
  });

  it("killNode sends SIGKILL and stops tracking the process", () => {
    const child = fakeChildProcess();
    (spawn as jest.Mock).mockReturnValue(child);

    const manager = new ClusterManager("src/node/CacheNode.ts", logger);
    manager.spawnNode({
      nodeId: "node-z",
      host: "127.0.0.1",
      port: 3012,
      gossipPort: 4012,
      seedHost: "127.0.0.1",
      seedPort: 3001,
    });

    manager.killNode("node-z");
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(manager.getRunningNodes()).toEqual([]);
  });

  it("stopAll sends SIGTERM to every tracked process and clears the set", () => {
    const childA = fakeChildProcess();
    const childB = fakeChildProcess();
    (spawn as jest.Mock).mockReturnValueOnce(childA).mockReturnValueOnce(childB);

    const manager = new ClusterManager("src/node/CacheNode.ts", logger);
    manager.spawnNode({ nodeId: "a", host: "127.0.0.1", port: 3020, gossipPort: 4020, seedHost: "127.0.0.1", seedPort: 3001 });
    manager.spawnNode({ nodeId: "b", host: "127.0.0.1", port: 3021, gossipPort: 4021, seedHost: "127.0.0.1", seedPort: 3001 });

    manager.stopAll();

    expect(childA.kill).toHaveBeenCalledWith("SIGTERM");
    expect(childB.kill).toHaveBeenCalledWith("SIGTERM");
    expect(manager.getRunningNodes()).toEqual([]);
  });
});
