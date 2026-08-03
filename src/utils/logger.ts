import pino from "pino";

export function createLogger(nodeId: string, level = "info") {
  return pino({
    level,
    base: { nodeId },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}

export type Logger = ReturnType<typeof createLogger>;
