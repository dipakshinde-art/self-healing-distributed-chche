import { CacheEntry } from "../types";

export class CacheStore {
  private store = new Map<string, CacheEntry>();
  private version = 0;

  set(key: string, value: string | number | Buffer, ttlMs?: number, nodeId = "local"): CacheEntry {
    const entry: CacheEntry = {
      key,
      value,
      expiresAt: ttlMs ? Date.now() + ttlMs : null,
      version: ++this.version,
      nodeId,
      createdAt: Date.now(),
    };
    this.store.set(key, entry);
    return entry;
  }

  get(key: string): CacheEntry | null {
    const entry = this.store.get(key);
    if (!entry) return null;

    if (entry.expiresAt !== null && Date.now() > entry.expiresAt) {
      this.store.delete(key); // lazy eviction
      return null;
    }

    return entry;
  }

  delete(key: string): boolean {
    return this.store.delete(key);
  }

  has(key: string): boolean {
    return this.get(key) !== null;
  }

  // Import an entry directly (used during replication and key migration)
  importEntry(entry: CacheEntry): void {
    const existing = this.store.get(entry.key);
    // Last-write-wins: higher version wins
    if (!existing || entry.version > existing.version) {
      this.store.set(entry.key, entry);
    }
  }

  keys(): string[] {
    return Array.from(this.store.keys());
  }

  entries(): CacheEntry[] {
    return Array.from(this.store.values());
  }

  size(): number {
    return this.store.size;
  }

  // Rough heuristic, not an exact accounting — good enough to compare against
  // EVICTION_THRESHOLD_MB without pulling in a real memory profiler.
  estimateSizeBytes(): number {
    let total = 0;
    for (const [key, entry] of this.store) {
      total += key.length * 2;
      const value = entry.value;
      total += Buffer.isBuffer(value) ? value.byteLength : String(value).length * 2;
    }
    return total;
  }

  // Purge all expired keys — called by TTLEngine on a sweep
  purgeExpired(): number {
    const now = Date.now();
    let purged = 0;

    for (const [key, entry] of this.store) {
      if (entry.expiresAt !== null && now > entry.expiresAt) {
        this.store.delete(key);
        purged++;
      }
    }

    return purged;
  }
}
