import { withRetry } from "../../src/utils/retry";

describe("withRetry", () => {
  it("returns the result immediately on first success, without retrying", async () => {
    const fn = jest.fn().mockResolvedValue("ok");
    const result = await withRetry(fn, 3, 1);
    expect(result).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("retries after a failure and returns the eventual success", async () => {
    const fn = jest
      .fn()
      .mockRejectedValueOnce(new Error("fail once"))
      .mockResolvedValueOnce("recovered");

    const result = await withRetry(fn, 3, 1);
    expect(result).toBe("recovered");
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("throws the last error once retries are exhausted", async () => {
    const fn = jest.fn().mockRejectedValue(new Error("always fails"));
    await expect(withRetry(fn, 2, 1)).rejects.toThrow("always fails");
    expect(fn).toHaveBeenCalledTimes(3); // initial attempt + 2 retries
  });
});
