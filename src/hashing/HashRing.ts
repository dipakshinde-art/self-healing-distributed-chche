import { MembershipEntry, VNode } from "../types";
import { hash } from "./murmur";

export class HashRing {
  private ring: VNode[] = []; // sorted by position ascending
  private nodeMap = new Map<string, MembershipEntry>();
  private readonly vnodeCount: number;

  constructor(vnodeCount = 150) {
    this.vnodeCount = vnodeCount;
  }

  addNode(entry: MembershipEntry): void {
    // Idempotent: a node already on the ring just gets its metadata refreshed
    // (host/port/status/etc. can change across calls) — vnodes are never
    // re-inserted, or repeated calls (e.g. from both a direct join and a
    // gossip-driven membership update) would duplicate vnodes and skew that
    // node's share of the keyspace.
    if (this.nodeMap.has(entry.nodeId)) {
      this.nodeMap.set(entry.nodeId, entry);
      return;
    }

    this.nodeMap.set(entry.nodeId, entry);

    for (let i = 0; i < this.vnodeCount; i++) {
      const position = hash(`${entry.nodeId}#${i}`);
      this.ring.push({ position, nodeId: entry.nodeId });
    }

    this.ring.sort((a, b) => a.position - b.position);
  }

  removeNode(nodeId: string): void {
    this.nodeMap.delete(nodeId);
    this.ring = this.ring.filter((v) => v.nodeId !== nodeId);
  }

  // Returns the primary node that owns this key
  getPrimaryNode(key: string): MembershipEntry | null {
    if (this.ring.length === 0) return null;

    const pos = hash(key);
    const idx = this.findClockwiseIndex(pos);
    const nodeId = this.ring[idx].nodeId;
    return this.nodeMap.get(nodeId) ?? null;
  }

  // Returns RF nodes (primary + replicas) for a key, skipping duplicates
  getReplicaNodes(key: string, rf: number): MembershipEntry[] {
    if (this.ring.length === 0) return [];

    const pos = hash(key);
    const startIdx = this.findClockwiseIndex(pos);
    const seen = new Set<string>();
    const result: MembershipEntry[] = [];

    for (let i = 0; i < this.ring.length && result.length < rf; i++) {
      const idx = (startIdx + i) % this.ring.length;
      const { nodeId } = this.ring[idx];
      if (!seen.has(nodeId)) {
        seen.add(nodeId);
        const entry = this.nodeMap.get(nodeId);
        if (entry) result.push(entry);
      }
    }

    return result;
  }

  // Returns all keys that should migrate to a newly added node
  getKeysForNode(allKeys: string[], nodeId: string): string[] {
    return allKeys.filter((key) => this.getPrimaryNode(key)?.nodeId === nodeId);
  }

  getNodeCount(): number {
    return this.nodeMap.size;
  }

  getNodes(): MembershipEntry[] {
    return Array.from(this.nodeMap.values());
  }

  private findClockwiseIndex(position: number): number {
    let lo = 0;
    let hi = this.ring.length - 1;

    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.ring[mid].position < position) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    // Wrap around if position is beyond the last vnode
    return this.ring[lo].position >= position ? lo : 0;
  }
}
