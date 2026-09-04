import { CacheStore } from "./CacheStore";
import { EvictionTracker } from "./EvictionPolicy";
import { Logger } from "../utils/logger";

const CANDIDATES_PER_BATCH = 50;
const MAX_BATCHES_PER_SWEEP = 10; // safety cap so one sweep can't run away

// Triggers the eviction ranking that EvictionTracker already maintains.
// Tracking access/recency is cheap and always-on; actually deleting keys
// only happens here, when the node is over its configured memory threshold.
export class EvictionSweeper {
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private store: CacheStore,
    private eviction: EvictionTracker,
    private thresholdMb: number,
    private sweepIntervalMs: number,
    private logger: Logger
  ) {}

  start(): void {
    this.timer = setInterval(() => this.sweep(), this.sweepIntervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private sweep(): void {
    const thresholdBytes = this.thresholdMb * 1024 * 1024;
    let evicted = 0;

    for (let batch = 0; batch < MAX_BATCHES_PER_SWEEP; batch++) {
      if (this.store.estimateSizeBytes() <= thresholdBytes) break;

      const candidates = this.eviction.getEvictionCandidates(this.store, CANDIDATES_PER_BATCH);
      if (candidates.length === 0) break;

      for (const key of candidates) {
        if (this.store.delete(key)) {
          this.eviction.recordDelete(key);
          evicted++;
        }
      }
    }

    if (evicted > 0) {
      this.logger.info({ evicted }, "Eviction sweep freed memory-pressure keys");
    }
  }
}
