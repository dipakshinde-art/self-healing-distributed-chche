import { MembershipEntry, NodeStatus } from "../types";

export class MembershipList {
  private members = new Map<string, MembershipEntry>();

  add(entry: MembershipEntry): void {
    this.members.set(entry.nodeId, entry);
  }

  updateStatus(nodeId: string, status: NodeStatus): void {
    const entry = this.members.get(nodeId);
    if (entry) {
      entry.status = status;
    }
  }

  updateLastSeen(nodeId: string, term: number): void {
    const entry = this.members.get(nodeId);
    if (entry) {
      entry.lastSeen = Date.now();
      entry.term = Math.max(entry.term, term);
    }
  }

  // Merge incoming gossip — higher term wins per node
  merge(incoming: MembershipEntry[]): string[] {
    const changed: string[] = [];

    for (const remote of incoming) {
      const local = this.members.get(remote.nodeId);
      if (!local || remote.term > local.term) {
        this.members.set(remote.nodeId, { ...remote });
        changed.push(remote.nodeId);
      }
    }

    return changed;
  }

  getAll(): MembershipEntry[] {
    return Array.from(this.members.values());
  }

  getAlive(): MembershipEntry[] {
    return this.getAll().filter((m) => m.status === "ALIVE");
  }

  get(nodeId: string): MembershipEntry | undefined {
    return this.members.get(nodeId);
  }

  remove(nodeId: string): void {
    this.members.delete(nodeId);
  }

  size(): number {
    return this.members.size;
  }
}
