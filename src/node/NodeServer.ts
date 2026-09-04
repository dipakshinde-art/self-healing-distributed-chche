import http from "http";
import { CacheStore } from "../storage/CacheStore";
import { ReplicationManager } from "../replication/ReplicationManager";
import { KeyMigrator } from "../replication/KeyMigrator";
import { HashRing } from "../hashing/HashRing";
import { MembershipList } from "../gossip/MembershipList";
import { EvictionTracker } from "../storage/EvictionPolicy";
import { Logger } from "../utils/logger";
import {
  ConsistencyMode,
  JoinRequest,
  JoinResponse,
  MembershipEntry,
  MigrationBatch,
  ReplicationPayload,
} from "../types";

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
    private logger: Logger,
    private forwardTimeoutMs = 1500
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
      const consistency = normalizeConsistency(req.headers["x-consistency"]);

      // ── Client-facing routes — forwarded to the real owner if this node isn't one ──
      // Every branch below is `await`ed (never a bare `return this.someAsyncFn(...)`)
      // so a rejection inside it is caught by this function's own try/catch instead
      // of escaping as an unhandled rejection, which can crash the whole process.
      if (req.method === "GET" && url.pathname.startsWith("/get/")) {
        const key = decodeURIComponent(url.pathname.slice(5));
        return await this.resolveGet(key, res);
      }

      if (req.method === "POST" && url.pathname === "/set") {
        const body = await readBody(req);
        const { key } = JSON.parse(body);
        if (typeof key === "string" && this.isOwner(key)) return await this.handleSet(body, res, consistency);
        return await this.forwardSet(body, res, consistency);
      }

      if (req.method === "DELETE" && url.pathname.startsWith("/del/")) {
        const key = decodeURIComponent(url.pathname.slice(5));
        if (this.isOwner(key)) return this.handleDelete(key, res);
        return await this.forwardDelete(key, res);
      }

      // ── Internal, non-forwarding routes — always execute locally ────────────────
      // A single hop lands here: these never re-check ownership or forward again,
      // which is what bounds forwarding to exactly one hop with no loop-guard needed.
      if (req.method === "GET" && url.pathname.startsWith("/internal/get/")) {
        const key = decodeURIComponent(url.pathname.slice("/internal/get/".length));
        return this.handleGet(key, res);
      }

      if (req.method === "POST" && url.pathname === "/internal/set") {
        const body = await readBody(req);
        return await this.handleSet(body, res, consistency);
      }

      if (req.method === "DELETE" && url.pathname.startsWith("/internal/del/")) {
        const key = decodeURIComponent(url.pathname.slice("/internal/del/".length));
        return this.handleDelete(key, res);
      }

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

      if (req.method === "GET" && url.pathname === "/status") {
        return json(res, 200, {
          nodeId: this.nodeId,
          keys: this.store.size(),
          members: this.membership.getAll(),
        });
      }

      json(res, 404, { error: "Not found" });
    } catch (err) {
      this.logger.error({ err }, "Request handler error");
      json(res, 500, { error: "Internal error" });
    }
  }

  // Does this node's own (gossip-maintained) ring view make it a legitimate
  // primary/replica owner of key? Every node computes this the same way from
  // the same ring, so any node is a valid, correct entry point for any key.
  private isOwner(key: string): boolean {
    return this.ownerCandidates(key).some((n) => n.nodeId === this.nodeId);
  }

  private ownerCandidates(key: string): MembershipEntry[] {
    return this.ring.getReplicaNodes(key, this.replicationFactor);
  }

  private handleGet(key: string, res: http.ServerResponse): void {
    const entry = this.store.get(key);
    if (!entry) return json(res, 404, { error: "Key not found" });
    this.eviction.recordAccess(key);
    json(res, 200, { key: entry.key, value: entry.value, version: entry.version });
  }

  private async handleSet(
    rawBody: string,
    res: http.ServerResponse,
    consistency: ConsistencyMode = "eventual"
  ): Promise<void> {
    const { key, value, ttl } = JSON.parse(rawBody);
    if (!key || value === undefined) return json(res, 400, { error: "key and value required" });

    const entry = this.store.set(key, value, ttl, this.nodeId);
    this.eviction.recordSet(key);

    const replicas = this.ring.getReplicaNodes(key, this.replicationFactor).filter(
      (n) => n.nodeId !== this.nodeId
    );

    if (consistency === "strong" && replicas.length > 0) {
      const quorum = Math.floor(this.replicationFactor / 2) + 1;
      const { acked } = await this.replication.replicateToNodes(entry, replicas);
      const totalAcked = acked + 1; // local write counts as one ack
      if (totalAcked < quorum) {
        return json(res, 503, {
          ok: false,
          error: "Quorum not reached",
          acked: totalAcked,
          required: quorum,
        });
      }
      return json(res, 200, { ok: true, version: entry.version, consistency: "strong", acked: totalAcked });
    }

    // Eventual (default): don't block the client response on replication.
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

  // ── Forwarding: tried in ring order (primary, then replicas) so a SUSPECT/
  // slow node is skipped in favor of the next legitimate owner. ──────────────

  // Checks locally first (fast path, no network hop, whether or not this
  // node is a "legitimate" owner — a stale/orphaned local copy is still
  // correct data). If absent, walks every other owner candidate in ring
  // order. A clean 404 from a reachable owner only means *that* owner
  // hasn't got the key yet (e.g. it just became a nominal owner but a
  // pending migration/replication hasn't landed there) — not that the key
  // is truly absent cluster-wide — so 404s keep the search going too;
  // only a hit or a non-404 response stops it early.
  private async resolveGet(key: string, res: http.ServerResponse): Promise<void> {
    const local = this.store.get(key);
    if (local) {
      this.eviction.recordAccess(key);
      return json(res, 200, { key: local.key, value: local.value, version: local.version });
    }

    const others = this.ownerCandidates(key).filter((o) => o.nodeId !== this.nodeId);
    for (const owner of others) {
      const result = await this.proxy(owner, "GET", `/internal/get/${encodeURIComponent(key)}`);
      if (result && result.status !== 404) return json(res, result.status, result.body);
    }

    json(res, 404, { error: "Key not found" });
  }

  private async forwardSet(
    rawBody: string,
    res: http.ServerResponse,
    consistency: ConsistencyMode
  ): Promise<void> {
    const { key } = JSON.parse(rawBody);
    for (const owner of this.ownerCandidates(key)) {
      const result = await this.proxy(owner, "POST", "/internal/set", rawBody, consistency);
      if (result) return json(res, result.status, result.body);
    }
    json(res, 503, { error: "No reachable owner for key" });
  }

  private async forwardDelete(key: string, res: http.ServerResponse): Promise<void> {
    for (const owner of this.ownerCandidates(key)) {
      const result = await this.proxy(owner, "DELETE", `/internal/del/${encodeURIComponent(key)}`);
      if (result) return json(res, result.status, result.body);
    }
    json(res, 503, { error: "No reachable owner for key" });
  }

  private proxy(
    target: MembershipEntry,
    method: "GET" | "POST" | "DELETE",
    path: string,
    body?: string,
    consistency?: ConsistencyMode
  ): Promise<{ status: number; body: unknown } | null> {
    return new Promise((resolve) => {
      const headers: http.OutgoingHttpHeaders = {};
      if (body !== undefined) {
        headers["Content-Type"] = "application/json";
        headers["Content-Length"] = Buffer.byteLength(body);
      }
      if (consistency) headers["X-Consistency"] = consistency;

      const req = http.request(
        {
          host: target.host,
          port: target.port,
          path,
          method,
          headers,
          timeout: this.forwardTimeoutMs,
          agent: false, // avoid a stale pooled socket to a node that restarted on the same port
        },
        (res) => {
          let data = "";
          res.on("data", (chunk) => (data += chunk));
          res.on("end", () => {
            try {
              resolve({ status: res.statusCode ?? 502, body: data ? JSON.parse(data) : {} });
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
      if (body !== undefined) req.write(body);
      req.end();
    });
  }
}

function normalizeConsistency(header: string | string[] | undefined): ConsistencyMode {
  const value = Array.isArray(header) ? header[0] : header;
  return value?.toLowerCase() === "strong" ? "strong" : "eventual";
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
