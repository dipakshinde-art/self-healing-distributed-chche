import http from "http";
import { CacheEntry, MembershipEntry, ReplicationPayload } from "../types";
import { Logger } from "../utils/logger";

export class ReplicationManager {
  constructor(
    private nodeId: string,
    private logger: Logger
  ) {}

  // Replicates to every replica and reports how many acked — callers that
  // don't care (eventual mode) just ignore the result and let this resolve
  // in the background; callers that need a quorum (strong mode) await it.
  async replicateToNodes(
    entry: CacheEntry,
    replicas: MembershipEntry[]
  ): Promise<{ acked: number; attempted: number }> {
    const payload: ReplicationPayload = {
      key: entry.key,
      entry,
      sourceNodeId: this.nodeId,
    };

    const body = JSON.stringify(payload);

    const results = await Promise.allSettled(
      replicas.map((replica) =>
        this.sendReplication(replica, body).catch((err) => {
          this.logger.warn({ replica: replica.nodeId, err }, "Replication failed");
          throw err;
        })
      )
    );

    const acked = results.filter((r) => r.status === "fulfilled").length;
    return { acked, attempted: replicas.length };
  }

  private sendReplication(replica: MembershipEntry, body: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          host: replica.host,
          port: replica.port,
          path: "/internal/replicate",
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
          timeout: 2000,
          // Never reuse a pooled keep-alive socket: a replica that died and
          // came back on the same port must be reached by a fresh connection,
          // not a stale one left in the agent's pool.
          agent: false,
        },
        (res) => {
          res.resume(); // drain response
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
