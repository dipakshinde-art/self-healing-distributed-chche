import http from "http";
import { CacheEntry, MembershipEntry, ReplicationPayload } from "../types";
import { Logger } from "../utils/logger";

export class ReplicationManager {
  constructor(
    private nodeId: string,
    private logger: Logger
  ) {}

  // Fire-and-forget async replication to replica nodes
  async replicateToNodes(entry: CacheEntry, replicas: MembershipEntry[]): Promise<void> {
    const payload: ReplicationPayload = {
      key: entry.key,
      entry,
      sourceNodeId: this.nodeId,
    };

    const body = JSON.stringify(payload);

    await Promise.allSettled(
      replicas.map((replica) =>
        this.sendReplication(replica, body).catch((err) => {
          this.logger.warn({ replica: replica.nodeId, err }, "Replication failed");
        })
      )
    );
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
