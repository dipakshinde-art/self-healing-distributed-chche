import { EventEmitter } from "events";
import { MembershipList } from "./MembershipList";
import { Logger } from "../utils/logger";

// State machine: ALIVE → SUSPECT → DEAD
// Emits: "node:suspect", "node:dead", "node:alive"
export class NodeStateManager extends EventEmitter {
  private suspectTimers = new Map<string, NodeJS.Timeout>();
  private deadTimers = new Map<string, NodeJS.Timeout>();

  constructor(
    private membership: MembershipList,
    private suspectTimeoutMs: number,
    private deadTimeoutMs: number,
    private logger: Logger
  ) {
    super();
  }

  heartbeatReceived(nodeId: string, term: number): void {
    const entry = this.membership.get(nodeId);
    if (!entry) return;

    this.clearTimers(nodeId);
    this.membership.updateLastSeen(nodeId, term);

    if (entry.status !== "ALIVE") {
      this.membership.updateStatus(nodeId, "ALIVE");
      this.logger.info({ nodeId }, "Node recovered to ALIVE");
      this.emit("node:alive", nodeId);
    }
  }

  missedHeartbeat(nodeId: string): void {
    const entry = this.membership.get(nodeId);
    if (!entry || entry.status !== "ALIVE") return;

    this.membership.updateStatus(nodeId, "SUSPECT");
    this.logger.warn({ nodeId }, "Node marked SUSPECT");
    this.emit("node:suspect", nodeId);

    const deadTimer = setTimeout(() => {
      const current = this.membership.get(nodeId);
      if (current?.status === "SUSPECT") {
        this.membership.updateStatus(nodeId, "DEAD");
        this.logger.error({ nodeId }, "Node marked DEAD");
        this.emit("node:dead", nodeId);
      }
    }, this.deadTimeoutMs - this.suspectTimeoutMs);

    this.deadTimers.set(nodeId, deadTimer);
  }

  private clearTimers(nodeId: string): void {
    const s = this.suspectTimers.get(nodeId);
    if (s) { clearTimeout(s); this.suspectTimers.delete(nodeId); }

    const d = this.deadTimers.get(nodeId);
    if (d) { clearTimeout(d); this.deadTimers.delete(nodeId); }
  }

  destroy(): void {
    for (const t of this.suspectTimers.values()) clearTimeout(t);
    for (const t of this.deadTimers.values()) clearTimeout(t);
  }
}
