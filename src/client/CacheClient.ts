import http from "http";
import { HashRing } from "../hashing/HashRing";
import { MembershipList } from "../gossip/MembershipList";
import { MembershipEntry, SetOptions, GetOptions } from "../types";
import { withRetry } from "../utils/retry";

// Public-facing SDK: routes requests to the correct cluster node
export class CacheClient {
  constructor(
    private ring: HashRing,
    private membership: MembershipList,
    private maxRetries = 1,
    private retryDelayMs = 50
  ) {}

  async set(key: string, value: unknown, options: SetOptions = {}): Promise<void> {
    const nodes = this.ring.getReplicaNodes(key, 1);
    if (nodes.length === 0) throw new Error("No nodes available");

    await withRetry(
      () => this.httpPost(nodes[0], "/set", { key, value, ttl: options.ttl }),
      this.maxRetries,
      this.retryDelayMs
    );
  }

  async get(key: string, _options: GetOptions = {}): Promise<unknown | null> {
    const nodes = this.ring.getReplicaNodes(key, 2);
    if (nodes.length === 0) throw new Error("No nodes available");

    for (const node of nodes) {
      try {
        const result = await this.httpGet(node, `/get/${encodeURIComponent(key)}`);
        return result.value;
      } catch {
        // Try next replica
      }
    }

    return null;
  }

  async delete(key: string): Promise<void> {
    const nodes = this.ring.getReplicaNodes(key, 1);
    if (nodes.length === 0) throw new Error("No nodes available");

    await withRetry(
      () => this.httpDelete(nodes[0], `/del/${encodeURIComponent(key)}`),
      this.maxRetries,
      this.retryDelayMs
    );
  }

  private httpGet(node: MembershipEntry, path: string): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const req = http.request(
        { host: node.host, port: node.port, path, method: "GET", timeout: 2000 },
        (res) => {
          let data = "";
          res.on("data", (c) => (data += c));
          res.on("end", () => {
            if (res.statusCode === 200) resolve(JSON.parse(data));
            else reject(new Error(`HTTP ${res.statusCode}`));
          });
        }
      );
      req.on("error", reject);
      req.on("timeout", () => { req.destroy(); reject(new Error("Timeout")); });
      req.end();
    });
  }

  private httpPost(node: MembershipEntry, path: string, body: unknown): Promise<void> {
    const payload = JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          host: node.host,
          port: node.port,
          path,
          method: "POST",
          headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) },
          timeout: 2000,
        },
        (res) => {
          res.resume();
          res.statusCode === 200 ? resolve() : reject(new Error(`HTTP ${res.statusCode}`));
        }
      );
      req.on("error", reject);
      req.on("timeout", () => { req.destroy(); reject(new Error("Timeout")); });
      req.write(payload);
      req.end();
    });
  }

  private httpDelete(node: MembershipEntry, path: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const req = http.request(
        { host: node.host, port: node.port, path, method: "DELETE", timeout: 2000 },
        (res) => {
          res.resume();
          res.statusCode === 200 ? resolve() : reject(new Error(`HTTP ${res.statusCode}`));
        }
      );
      req.on("error", reject);
      req.on("timeout", () => { req.destroy(); reject(new Error("Timeout")); });
      req.end();
    });
  }
}
