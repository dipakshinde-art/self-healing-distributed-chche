import { LRUEviction, LFUEviction, NoopEviction, createEvictionTracker, EvictableStore } from "../../src/storage/EvictionPolicy";

function fakeStore(keys: string[]): EvictableStore {
  return {
    keys: () => keys,
    delete: () => true,
  };
}

describe("LRUEviction", () => {
  it("evicts the least recently used key first", () => {
    const lru = new LRUEviction();
    lru.recordSet("a");
    lru.recordSet("b");
    lru.recordSet("c");

    expect(lru.getEvictionCandidates(fakeStore(["a", "b", "c"]), 1)).toEqual(["a"]);
  });

  it("recordAccess refreshes recency so the key is no longer the eviction candidate", () => {
    const lru = new LRUEviction();
    lru.recordSet("a");
    lru.recordSet("b");
    lru.recordAccess("a"); // touching "a" makes "b" the oldest now

    expect(lru.getEvictionCandidates(fakeStore(["a", "b"]), 1)).toEqual(["b"]);
  });

  it("never returns a key the store no longer has (e.g. expired via TTL)", () => {
    const lru = new LRUEviction();
    lru.recordSet("a");
    lru.recordSet("b");

    expect(lru.getEvictionCandidates(fakeStore(["b"]), 2)).toEqual(["b"]);
  });

  it("recordDelete removes a key from recency tracking", () => {
    const lru = new LRUEviction();
    lru.recordSet("a");
    lru.recordSet("b");
    lru.recordDelete("a");

    expect(lru.getEvictionCandidates(fakeStore(["b"]), 5)).toEqual(["b"]);
  });
});

describe("LFUEviction", () => {
  it("evicts the least frequently accessed key first", () => {
    const lfu = new LFUEviction();
    lfu.recordSet("a");
    lfu.recordSet("b");
    lfu.recordAccess("a");
    lfu.recordAccess("a");
    lfu.recordAccess("b");

    expect(lfu.getEvictionCandidates(fakeStore(["a", "b"]), 1)).toEqual(["b"]);
  });

  it("filters out keys no longer present in the store", () => {
    const lfu = new LFUEviction();
    lfu.recordSet("a");
    lfu.recordSet("b");
    lfu.recordAccess("b");

    expect(lfu.getEvictionCandidates(fakeStore(["b"]), 5)).toEqual(["b"]);
  });
});

describe("NoopEviction", () => {
  it("never proposes eviction candidates", () => {
    const noop = new NoopEviction();
    noop.recordSet("a");
    noop.recordAccess("a");
    expect(noop.getEvictionCandidates(fakeStore(["a"]), 5)).toEqual([]);
  });
});

describe("createEvictionTracker", () => {
  it("builds the tracker matching the configured policy", () => {
    expect(createEvictionTracker("lru")).toBeInstanceOf(LRUEviction);
    expect(createEvictionTracker("lfu")).toBeInstanceOf(LFUEviction);
    expect(createEvictionTracker("none")).toBeInstanceOf(NoopEviction);
  });
});
