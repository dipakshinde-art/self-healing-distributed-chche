// Stretch Goal: LRU/LFU eviction under memory pressure

export interface EvictableStore {
  keys(): string[];
  delete(key: string): boolean;
}

// TODO Phase 8 — LRU eviction using DoublyLinkedList + HashMap for O(1) ops
export class LRUEviction {
  private capacity: number;
  // Implementation: doubly-linked list + map

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  // Returns keys to evict given current memory pressure
  getEvictionCandidates(_store: EvictableStore, _count: number): string[] {
    // TODO: implement LRU order tracking
    return [];
  }
}

// TODO Phase 8 — LFU eviction
export class LFUEviction {
  getEvictionCandidates(_store: EvictableStore, _count: number): string[] {
    // TODO: implement frequency counter + min-heap
    return [];
  }
}
