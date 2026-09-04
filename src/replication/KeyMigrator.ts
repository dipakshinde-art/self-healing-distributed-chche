import http from "http";
import { CacheEntry, MembershipEntry, MigrationBatch } from "../types";
import { CacheStore } from "../storage/CacheStore";
import { Logger } from "../utils/logger";
import { withRetry } from "../utils/retry";

const BATCH_SIZE = 100;
const BATCH_MAX_RETRIES = 3;
const BATCH_RETRY_DELAY_MS = 200;

export class KeyMigrator {
  constructor(
    private store: CacheStore,
    private nodeId: string,
    private logger: Logger
  ) {}

  // Stream keys in batches to a target node, then delete locally
  async migrateKeys(keys: string[], target: MembershipEntry): Promise<void> {
    const entries: CacheEntry[] = keys
      .map((k) => this.store.get(k))
      .filter((e): e is CacheEntry => e !== null);

    this.logger.info({ count: entries.length, target: target.nodeId }, "Starting key migration");

    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);
      await withRetry(() => this.sendBatch(batch, target), BATCH_MAX_RETRIES, BATCH_RETRY_DELAY_MS);

      // Delete migrated keys from this node after confirmed receipt
      for (const entry of batch) {
        this.store.delete(entry.key);
      }

      this.logger.debug({ sent: i + batch.length, total: entries.length }, "Migration progress");
    }

    this.logger.info({ target: target.nodeId }, "Key migration complete");
  }

  private sendBatch(entries: CacheEntry[], target: MembershipEntry): Promise<void> {
    const payload: MigrationBatch = {
      entries,
      sourceNodeId: this.nodeId,
      targetNodeId: target.nodeId,
    };

    const body = JSON.stringify(payload);

    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          host: target.host,
          port: target.port,
          path: "/internal/import-keys",
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
          timeout: 10000,
          agent: false, // avoid a stale pooled socket to a node that restarted on the same port
        },
        (res) => {
          res.resume();
          res.statusCode === 200 ? resolve() : reject(new Error(`HTTP ${res.statusCode}`));
        }
      );
      req.on("error", reject);
      req.on("timeout", () => { req.destroy(); reject(new Error("Timeout")); });
      req.write(body);
      req.end();
    });
  }
}
