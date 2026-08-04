import http from "http";
import { CacheStore } from "../storage/CacheStore";
import { ReplicationManager } from "../replication/ReplicationManager";
import { KeyMigrator } from "../replication/KeyMigrator";
import { HashRing } from "../hashing/HashRing";
import { MembershipList } from "../gossip/MembershipList";
import { EvictionTracker } from "../storage/EvictionPolicy";
import { Logger } from "../utils/logger";
import { JoinRequest, JoinResponse, MigrationBatch, ReplicationPayload } from "../types";

export class NodeServer {
  private server: http.Server;

  constructor(
    private nodeId: string,
    private port: number,
    private store: CacheStore,
    private ring: HashRing,
    private replication: ReplicationManager,
    private replicationFactor: number,
    private membership: MembershipList,
    private migrator: KeyMigrator,
    private eviction: EvictionTracker,
    private logger: Logger
  ) {
    this.server = http.createServer((req, res) => this.handle(req, res));
  }

  start(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(this.port, () => {
        this.logger.info({ port: this.port }, "Node HTTP server listening");
        resolve();
      });
    });
  }

  stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.close((err) => (err ? reject(err) : resolve()));
    });
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    try {
      const url = new URL(req.url ?? "/", `http://localhost`);

      // ── Client-facing routes ──────────────────────────────────────────────
      if (req.method === "GET" && url.pathname.startsWith("/get/")) {
        const key = decodeURIComponent(url.pathname.slice(5));
        return this.handleGet(key, res);
      }

      if (req.method === "POST" && url.pathname === "/set") {
        const body = await readBody(req);
        return this.handleSet(body, res);
      }

      if (req.method === "DELETE" && url.pathname.startsWith("/del/")) {
        const key = decodeURIComponent(url.pathname.slice(5));
        return this.handleDelete(key, res);
      }

      // ── Internal routes (node-to-node) ───────────────────────────────────
      if (req.method === "POST" && url.pathname === "/internal/replicate") {
        const body = await readBody(req);
        return this.handleReplicate(body, res);
      }

      if (req.method === "POST" && url.pathname === "/internal/import-keys") {
        const body = await readBody(req);
        return this.handleImportKeys(body, res);
      }

      if (req.method === "POST" && url.pathname === "/internal/join") {
        const body = await readBody(req);
        return this.handleJoin(body, res);
      }

      if (req.method === "GET" && url.pathname === "/health") {
        return json(res, 200, { nodeId: this.nodeId, status: "ALIVE", keys: this.store.size() });
      }

      json(res, 404, { error: "Not found" });
    } catch (err) {
      this.logger.error({ err }, "Request handler error");
      json(res, 500, { error: "Internal error" });
    }
  }

  private handleGet(key: string, res: http.ServerResponse): void {
    const entry = this.store.get(key);
    if (!entry) return json(res, 404, { error: "Key not found" });
    this.eviction.recordAccess(key);
    json(res, 200, { key: entry.key, value: entry.value, version: entry.version });
  }

  private async handleSet(rawBody: string, res: http.ServerResponse): Promise<void> {
    const { key, value, ttl } = JSON.parse(rawBody);
    if (!key || value === undefined) return json(res, 400, { error: "key and value required" });

    const entry = this.store.set(key, value, ttl, this.nodeId);
    this.eviction.recordSet(key);

    // Async replication to replicas — don't block client response
    const replicas = this.ring.getReplicaNodes(key, this.replicationFactor).filter(
      (n) => n.nodeId !== this.nodeId
    );
    this.replication.replicateToNodes(entry, replicas).catch(() => {});

    json(res, 200, { ok: true, version: entry.version });
  }

  private handleDelete(key: string, res: http.ServerResponse): void {
    const deleted = this.store.delete(key);
    if (deleted) this.eviction.recordDelete(key);
    json(res, 200, { ok: deleted });
  }

  private handleReplicate(rawBody: string, res: http.ServerResponse): void {
    const payload: ReplicationPayload = JSON.parse(rawBody);
    this.store.importEntry(payload.entry);
    json(res, 200, { ok: true });
  }

  private handleImportKeys(rawBody: string, res: http.ServerResponse): void {
    const batch: MigrationBatch = JSON.parse(rawBody);
    for (const entry of batch.entries) {
      this.store.importEntry(entry);
    }
    json(res, 200, { ok: true, imported: batch.entries.length });
  }

  // Seed-side of cluster bootstrap: admit the joining node into membership +
  // ring, hand back the full membership list, then migrate any keys the new
  // node now owns so it isn't serving empty responses for its share of the ring.
  private handleJoin(rawBody: string, res: http.ServerResponse): void {
    const { entry } = JSON.parse(rawBody) as JoinRequest;
    const isNewNode = !this.membership.get(entry.nodeId);

    this.membership.add(entry);
    this.ring.addNode(entry);

    const response: JoinResponse = { members: this.membership.getAll() };
    json(res, 200, response);
    this.logger.info({ nodeId: entry.nodeId }, "Node joined cluster");

    if (isNewNode) {
      const keysForNode = this.ring.getKeysForNode(this.store.keys(), entry.nodeId);
      if (keysForNode.length > 0) {
        this.migrator.migrateKeys(keysForNode, entry).catch((err) =>
          this.logger.warn({ err, target: entry.nodeId }, "Migration on join failed")
        );
      }
    }
  }
}

async function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function json(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}
