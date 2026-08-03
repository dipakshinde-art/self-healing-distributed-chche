// Thin wrapper around murmurhash3js — returns a uint32 position on the ring
// Install: npm install murmurhash3js

// eslint-disable-next-line @typescript-eslint/no-require-imports
const murmur = require("murmurhash3js");

export function hash(key: string): number {
  return murmur.x86.hash32(key, 0x9747b28c) >>> 0; // unsigned 32-bit
}
