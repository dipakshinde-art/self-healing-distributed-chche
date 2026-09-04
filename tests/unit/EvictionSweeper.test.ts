import { CacheStore } from "../../src/storage/CacheStore";
import { LRUEviction } from "../../src/storage/EvictionPolicy";
import { EvictionSweeper } from "../../src/storage/EvictionSweeper";
import { createLogger } from "../../src/utils/logger";

const logger = createLogger("test", "silent");

describe("EvictionSweeper", () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it("evicts the least-recently-used keys first once over the memory threshold", () => {
    jest.useFakeTimers();

    const store = new CacheStore();
    const eviction = new LRUEviction();

    // 60 keys, evenly sized: 50 (EvictionSweeper's batch size) is evicted in
    // one sweep once over threshold, leaving exactly the 10 most-recent.
    const NUM_KEYS = 60;
    const KEEP = 10;
    for (let i = 0; i < NUM_KEYS; i++) {
      const key = `k${i}`;
      store.set(key, "x".repeat(100));
      eviction.recordSet(key);
    }

    const perKeyBytes = store.estimateSizeBytes() / NUM_KEYS;
    const thresholdMb = (perKeyBytes * (KEEP + 2)) / (1024 * 1024); // just above what KEEP keys weigh

    const sweeper = new EvictionSweeper(store, eviction, thresholdMb, 100, logger);
    sweeper.start();
    jest.advanceTimersByTime(100);

    expect(store.size()).toBe(KEEP);
    for (let i = 0; i < NUM_KEYS - KEEP; i++) expect(store.get(`k${i}`)).toBeNull();
    for (let i = NUM_KEYS - KEEP; i < NUM_KEYS; i++) expect(store.get(`k${i}`)).not.toBeNull();

    sweeper.stop();
  });

  it("does nothing while under the memory threshold", () => {
    jest.useFakeTimers();

    const store = new CacheStore();
    const eviction = new LRUEviction();
    store.set("a", "small");
    eviction.recordSet("a");

    const sweeper = new EvictionSweeper(store, eviction, 256, 100, logger);
    sweeper.start();
    jest.advanceTimersByTime(300);

    expect(store.get("a")).not.toBeNull();
    sweeper.stop();
  });
});
