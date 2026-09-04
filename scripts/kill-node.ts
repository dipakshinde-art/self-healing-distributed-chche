// Phase 7 demo: kill a random (or specific) live cluster node mid-traffic.
// Run with: npx tsx scripts/kill-node.ts [--node-id node-2]
//
// Uses the .pids/<nodeId>.pid file each node writes on startup rather than
// parsing `netstat`/`taskkill`/`lsof` output — process.kill(pid, "SIGKILL")
// works identically on Windows (mapped to TerminateProcess), macOS, and Linux.
import fs from "fs";
import path from "path";

const PID_DIR = path.join(process.cwd(), ".pids");

function parseNodeIdArg(): string | null {
  const idx = process.argv.indexOf("--node-id");
  return idx !== -1 ? process.argv[idx + 1] ?? null : null;
}

function listLivePidFiles(): { nodeId: string; pid: number }[] {
  if (!fs.existsSync(PID_DIR)) return [];

  return fs
    .readdirSync(PID_DIR)
    .filter((f) => f.endsWith(".pid"))
    .map((f) => {
      const nodeId = f.slice(0, -4);
      const pid = Number(fs.readFileSync(path.join(PID_DIR, f), "utf8").trim());
      return { nodeId, pid };
    })
    .filter(({ pid }) => isProcessAlive(pid));
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0); // signal 0: existence check only, doesn't actually kill
    return true;
  } catch {
    return false;
  }
}

function main() {
  const requestedNodeId = parseNodeIdArg();
  const candidates = listLivePidFiles();

  if (candidates.length === 0) {
    console.error("No live node PID files found in .pids/ — is the cluster running?");
    process.exit(1);
  }

  const target = requestedNodeId
    ? candidates.find((c) => c.nodeId === requestedNodeId)
    : candidates[Math.floor(Math.random() * candidates.length)];

  if (!target) {
    console.error(`No live node found with nodeId "${requestedNodeId}". Live nodes: ${candidates.map((c) => c.nodeId).join(", ")}`);
    process.exit(1);
  }

  console.log(`Killing ${target.nodeId} (pid ${target.pid})...`);
  process.kill(target.pid, "SIGKILL");
  console.log(`${target.nodeId} killed. The cluster should detect this and route around it within DEAD_TIMEOUT_MS.`);
}

main();
