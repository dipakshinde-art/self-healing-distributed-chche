import dgram from "dgram";
import { EventEmitter } from "events";
import { GossipMessage, MembershipEntry } from "../types";
import { MembershipList } from "./MembershipList";
import { NodeStateManager } from "./NodeStateManager";
import { Logger } from "../utils/logger";

export class GossipAgent extends EventEmitter {
  private socket: dgram.Socket;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private term = 0;

  constructor(
    private nodeId: string,
    private host: string,
    private gossipPort: number,
    private membership: MembershipList,
    private stateManager: NodeStateManager,
    private fanout: number,
    private intervalMs: number,
    private logger: Logger
  ) {
    super();
    this.socket = dgram.createSocket("udp4");
  }

  async start(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.socket.bind(this.gossipPort, this.host, () => resolve());
      this.socket.once("error", reject);
    });

    this.socket.on("message", (buf) => this.handleMessage(buf));
    this.logger.info({ port: this.gossipPort }, "Gossip agent listening");

    this.heartbeatTimer = setInterval(() => this.tick(), this.intervalMs);
  }

  stop(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.socket.close();
    this.stateManager.destroy();
  }

  private tick(): void {
    this.term++;
    const peers = this.pickRandomPeers(this.fanout);

    const msg: GossipMessage = {
      type: "HEARTBEAT",
      from: this.nodeId,
      term: this.term,
      members: this.membership.getAll(),
      timestamp: Date.now(),
    };

    const buf = Buffer.from(JSON.stringify(msg));

    for (const peer of peers) {
      this.socket.send(buf, peer.gossipPort, peer.host, (err) => {
        if (err) this.logger.warn({ peer: peer.nodeId, err }, "Gossip send failed");
      });
    }

    // Check for nodes that haven't been seen recently
    const now = Date.now();
    for (const member of this.membership.getAll()) {
      if (member.nodeId === this.nodeId) continue;
      if (member.status === "ALIVE" && now - member.lastSeen > this.intervalMs * 2) {
        this.stateManager.missedHeartbeat(member.nodeId);
      }
    }
  }

  private handleMessage(buf: Buffer): void {
    try {
      const msg: GossipMessage = JSON.parse(buf.toString());

      if (msg.type === "HEARTBEAT" || msg.type === "STATE_UPDATE") {
        this.stateManager.heartbeatReceived(msg.from, msg.term);
        const changed = this.membership.merge(msg.members);
        if (changed.length > 0) {
          this.emit("membership:changed", changed);
        }
      }
    } catch {
      this.logger.warn("Failed to parse gossip message");
    }
  }

  private pickRandomPeers(count: number): MembershipEntry[] {
    const alive = this.membership.getAlive().filter((m) => m.nodeId !== this.nodeId);
    const shuffled = alive.sort(() => Math.random() - 0.5);
    return shuffled.slice(0, count);
  }
}
