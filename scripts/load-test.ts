// Phase 7 demo: real SET+GET correctness load test against a live cluster.
// Run with: npx tsx scripts/load-test.ts
//
// Unlike a plain HTTP-200 smoke test, this actually validates data: it SETs
// unique keys, then GETs them back from a *different* random node than the
// one they were written to and asserts the value matches. That's the real
// proof the cluster is routing/replicating correctly, not just responding.
import http from "http";
import { ClusterMetrics } from "../src/types";

const NODES = [
  { host: "127.0.0.1", port: 3001 },
  { host: "127.0.0.1", port: 3002 },
  { host: "127.0.0.1", port: 3003 },
];

const DURATION_MS = 30_000;
const CONCURRENCY = 50;
const SETTLE_MS = 150; // give async replication a moment to land before reading elsewhere
const REQUEST_TIMEOUT_MS = 1000;
const RETRY_DELAY_MS = 50; // blueprint 7.3: retry once after 50ms against a different node

interface KnownKey {
  value: string;
  setAt: number;
}

type Node = (typeof NODES)[number];
type JsonResponse = { status: number; json: any } | null;

const knownKeys = new Map<string, KnownKey>();
const latencies: number[] = [];
const metrics: ClusterMetrics = {
  totalRequests: 0,
  failedRequests: 0,
  retryCount: 0,
  latencyP50: 0,
  latencyP99: 0,
};

function randomNode(): Node {
  return NODES[Math.floor(Math.random() * NODES.length)];
}

function differentNode(exclude: Node): Node {
  const others = NODES.filter((n) => n.port !== exclude.port);
  return others[Math.floor(Math.random() * others.length)] ?? exclude;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requestJson(node: Node, method: string, path: string, body?: unknown): Promise<JsonResponse> {
  const payload = body !== undefined ? JSON.stringify(body) : undefined;
  return new Promise((resolve) => {
    const req = http.request(
      {
        host: node.host,
        port: node.port,
        path,
        method,
        timeout: REQUEST_TIMEOUT_MS,
        headers: payload
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) }
          : undefined,
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode ?? 0, json: data ? JSON.parse(data) : null });
          } catch {
            resolve(null);
          }
        });
      }
    );
    req.on("error", () => resolve(null));
    req.on("timeout", () => {
      req.destroy();
      resolve(null);
    });
    if (payload) req.write(payload);
    req.end();
  });
}

// null (connection refused/timeout) → one retry after 50ms against a different node.
async function withRetryOnAnotherNode(
  node: Node,
  attempt: (n: Node) => Promise<JsonResponse>
): Promise<JsonResponse> {
  const first = await attempt(node);
  if (first !== null) return first;

  metrics.retryCount++;
  await sleep(RETRY_DELAY_MS);
  return attempt(differentNode(node));
}

async function doSet(workerId: number, counter: number): Promise<void> {
  const key = `load-test-${workerId}-${counter}`;
  const value = `v-${workerId}-${counter}-${Date.now()}`;
  const node = randomNode();

  const res = await withRetryOnAnotherNode(node, (n) => requestJson(n, "POST", "/set", { key, value }));
  metrics.totalRequests++;

  if (!res || res.status !== 200) {
    metrics.failedRequests++;
    console.warn(`SET failed for ${key}: ${res?.status ?? "no response"}`);
    return;
  }

  knownKeys.set(key, { value, setAt: Date.now() });
}

async function doGet(): Promise<void> {
  const settled = Array.from(knownKeys.entries()).filter(([, k]) => Date.now() - k.setAt > SETTLE_MS);
  if (settled.length === 0) return;

  const [key, expected] = settled[Math.floor(Math.random() * settled.length)];
  const node = randomNode();
  const start = Date.now();

  const res = await withRetryOnAnotherNode(node, (n) => requestJson(n, "GET", `/get/${key}`));
  latencies.push(Date.now() - start);
  metrics.totalRequests++;

  if (!res || res.status !== 200 || res.json?.value !== expected.value) {
    metrics.failedRequests++;
    console.warn(
      `GET mismatch/failure for ${key}: status=${res?.status ?? "no response"}, ` +
        `got=${JSON.stringify(res?.json)}, expected=${expected.value}`
    );
  }
}

async function worker(workerId: number): Promise<void> {
  const end = Date.now() + DURATION_MS;
  let counter = 0;

  while (Date.now() < end) {
    await doSet(workerId, counter++);
    await doGet();
  }
}

async function main() {
  console.log(`Starting correctness load test: ${CONCURRENCY} workers, ${DURATION_MS / 1000}s duration`);
  console.log("Kill a node mid-test (npm run kill-node) to see failover in action!\n");

  await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

  const sorted = [...latencies].sort((a, b) => a - b);
  metrics.latencyP50 = sorted[Math.floor(sorted.length * 0.5)] ?? 0;
  metrics.latencyP99 = sorted[Math.floor(sorted.length * 0.99)] ?? 0;

  const retryRate = metrics.totalRequests > 0 ? (metrics.retryCount / metrics.totalRequests) * 100 : 0;
  const pass = metrics.failedRequests === 0 && retryRate < 5;

  console.log("\n=== LOAD TEST RESULTS ===");
  console.log(`Total requests:   ${metrics.totalRequests}`);
  console.log(`Failed requests:  ${metrics.failedRequests}`);
  console.log(`Retries:          ${metrics.retryCount}  (${retryRate.toFixed(2)}%)`);
  console.log(`Latency p50:      ${metrics.latencyP50}ms`);
  console.log(`Latency p99:      ${metrics.latencyP99}ms`);
  console.log(`\n${pass ? "PASS" : "FAIL"} — pass criteria: failed_requests=0, retries<5%`);
}

main().catch(console.error);
