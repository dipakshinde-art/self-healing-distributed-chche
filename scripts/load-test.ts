// Phase 7: 500 req/s load test — run with: npx tsx scripts/load-test.ts
import http from "http";

const NODES = [
  { host: "127.0.0.1", port: 3001 },
  { host: "127.0.0.1", port: 3002 },
  { host: "127.0.0.1", port: 3003 },
];

const DURATION_MS = 30_000;
const CONCURRENCY = 50;

let totalRequests = 0;
let failedRequests = 0;
let retryCount = 0;
const latencies: number[] = [];

function randomNode() {
  return NODES[Math.floor(Math.random() * NODES.length)];
}

function randomKey() {
  return `key-${Math.floor(Math.random() * 1000)}`;
}

async function doRequest(node: (typeof NODES)[0], key: string): Promise<void> {
  const start = Date.now();
  return new Promise((resolve) => {
    const req = http.request(
      { host: node.host, port: node.port, path: `/get/${key}`, method: "GET", timeout: 500 },
      (res) => {
        res.resume();
        latencies.push(Date.now() - start);
        if (res.statusCode !== 200 && res.statusCode !== 404) {
          failedRequests++;
        }
        resolve();
      }
    );
    req.on("error", () => {
      // Try retry on another node
      retryCount++;
      const altNode = NODES.find((n) => n.port !== node.port) ?? node;
      http.get({ host: altNode.host, port: altNode.port, path: `/get/${key}`, timeout: 500 }, (res) => {
        res.resume();
        if (res.statusCode !== 200 && res.statusCode !== 404) failedRequests++;
        resolve();
      }).on("error", () => { failedRequests++; resolve(); });
    });
    req.on("timeout", () => { req.destroy(); failedRequests++; resolve(); });
    req.end();
  });
}

async function worker() {
  const end = Date.now() + DURATION_MS;
  while (Date.now() < end) {
    await doRequest(randomNode(), randomKey());
    totalRequests++;
  }
}

async function main() {
  console.log(`Starting load test: ${CONCURRENCY} workers, ${DURATION_MS / 1000}s duration`);
  console.log("Kill a node mid-test to see failover in action!\n");

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  const sorted = latencies.sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];

  console.log("\n=== LOAD TEST RESULTS ===");
  console.log(`Total requests:   ${totalRequests}`);
  console.log(`Failed requests:  ${failedRequests}  (${((failedRequests / totalRequests) * 100).toFixed(2)}%)`);
  console.log(`Retries:          ${retryCount}  (${((retryCount / totalRequests) * 100).toFixed(2)}%)`);
  console.log(`Latency p50:      ${p50}ms`);
  console.log(`Latency p99:      ${p99}ms`);
  console.log("\nPASS criteria: failed_requests=0, retries<5%");
}

main().catch(console.error);
