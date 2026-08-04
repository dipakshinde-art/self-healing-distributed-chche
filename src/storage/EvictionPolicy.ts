// Stretch Goal: LRU/LFU eviction under memory pressure
import { EvictionPolicy as EvictionPolicyKind } from "../types";

export interface EvictableStore {
  keys(): string[];
  delete(key: string): boolean;
}

// Tracks access patterns as the store is used, so an eviction sweep has real
// data to rank candidates by. recordSet/recordAccess/recordDelete are meant
// to be called from the request layer (NodeServer) on every set/get/delete.
export interface EvictionTracker {
  recordSet(key: string): void;
  recordAccess(key: string): void;
  recordDelete(key: string): void;

  // Returns up to `count` keys to evict, ranked most-evictable first.
  // Always filters against store.keys() so a key that's already gone
  // (e.g. expired via TTL, deleted elsewhere) can never be returned stale.
  getEvictionCandidates(store: EvictableStore, count: number): string[];
}

// LRU eviction. A Map iterates in insertion order, and deleting + re-setting
// a key moves it to the end — so the map alone gives O(1) recency tracking
// without a hand-rolled doubly-linked list.
export class LRUEviction implements EvictionTracker {
  private recency = new Map<string, true>();

  recordSet(key: string): void {
    this.touch(key);
  }

  recordAccess(key: string): void {
    this.touch(key);
  }

  recordDelete(key: string): void {
    this.recency.delete(key);
  }

  getEvictionCandidates(store: EvictableStore, count: number): string[] {
    const alive = new Set(store.keys());
    const candidates: string[] = [];

    for (const key of this.recency.keys()) {
      if (candidates.length >= count) break;
      if (alive.has(key)) candidates.push(key);
    }

    return candidates;
  }

  private touch(key: string): void {
    this.recency.delete(key);
    this.recency.set(key, true);
  }
}

// LFU eviction — ranks keys by access count, least-frequently-used first.
export class LFUEviction implements EvictionTracker {
  private frequency = new Map<string, number>();

  recordSet(key: string): void {
    if (!this.frequency.has(key)) this.frequency.set(key, 0);
  }

  recordAccess(key: string): void {
    this.frequency.set(key, (this.frequency.get(key) ?? 0) + 1);
  }

  recordDelete(key: string): void {
    this.frequency.delete(key);
  }

  getEvictionCandidates(store: EvictableStore, count: number): string[] {
    const alive = new Set(store.keys());

    return Array.from(this.frequency.entries())
      .filter(([key]) => alive.has(key))
      .sort((a, b) => a[1] - b[1])
      .slice(0, count)
      .map(([key]) => key);
  }
}

// Used when EVICTION_POLICY=none — keeps NodeServer's call sites unconditional.
export class NoopEviction implements EvictionTracker {
  recordSet(_key: string): void {}
  recordAccess(_key: string): void {}
  recordDelete(_key: string): void {}
  getEvictionCandidates(_store: EvictableStore, _count: number): string[] {
    return [];
  }
}

export function createEvictionTracker(policy: EvictionPolicyKind): EvictionTracker {
  switch (policy) {
    case "lru":
      return new LRUEviction();
    case "lfu":
      return new LFUEviction();
    default:
      return new NoopEviction();
  }
}
