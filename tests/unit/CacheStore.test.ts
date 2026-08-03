import { CacheStore } from "../../src/storage/CacheStore";

describe("CacheStore", () => {
  let store: CacheStore;

  beforeEach(() => { store = new CacheStore(); });

  it("sets and gets a value", () => {
    store.set("name", "Alice");
    expect(store.get("name")?.value).toBe("Alice");
  });

  it("returns null for missing keys", () => {
    expect(store.get("missing")).toBeNull();
  });

  it("deletes a key", () => {
    store.set("x", 1);
    store.delete("x");
    expect(store.get("x")).toBeNull();
  });

  it("evicts expired keys on read (lazy eviction)", () => {
    jest.useFakeTimers();
    store.set("temp", "value", 1); // 1 second TTL

    jest.advanceTimersByTime(2000); // 2 seconds later

    expect(store.get("temp")).toBeNull();
    jest.useRealTimers();
  });

  it("returns value before TTL expires", () => {
    jest.useFakeTimers();
    store.set("temp", "value", 10); // 10 second TTL

    jest.advanceTimersByTime(5000); // 5 seconds later

    expect(store.get("temp")?.value).toBe("value");
    jest.useRealTimers();
  });

  it("importEntry uses last-write-wins by version", () => {
    store.set("k", "v1"); // version 1

    store.importEntry({ key: "k", value: "v2", version: 5, nodeId: "node-2", createdAt: Date.now(), expiresAt: null });
    expect(store.get("k")?.value).toBe("v2");

    // Lower version should NOT overwrite
    store.importEntry({ key: "k", value: "v-old", version: 2, nodeId: "node-3", createdAt: Date.now(), expiresAt: null });
    expect(store.get("k")?.value).toBe("v2");
  });

  it("purgeExpired removes all expired keys", () => {
    jest.useFakeTimers();
    store.set("a", 1, 1);
    store.set("b", 2, 1);
    store.set("c", 3, 100); // long TTL

    jest.advanceTimersByTime(2000);

    const purged = store.purgeExpired();
    expect(purged).toBe(2);
    expect(store.size()).toBe(1);
    jest.useRealTimers();
  });
});
