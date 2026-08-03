import { CacheStore } from "../storage/CacheStore";
import { Logger } from "../utils/logger";

export class TTLEngine {
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private store: CacheStore,
    private sweepIntervalMs: number,
    private logger: Logger
  ) {}

  start(): void {
    this.timer = setInterval(() => {
      const purged = this.store.purgeExpired();
      if (purged > 0) {
        this.logger.debug({ purged }, "TTL sweep purged expired keys");
      }
    }, this.sweepIntervalMs);

    this.timer.unref(); // don't keep process alive just for sweeps
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
