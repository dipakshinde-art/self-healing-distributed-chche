/** @type {import('jest').Config} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/tests"],
  moduleNameMapper: {
    "^../../config/(.*)$": "<rootDir>/config/$1",
  },
  collectCoverageFrom: ["src/**/*.ts"],
  coverageThreshold: {
    global: { branches: 70, functions: 70, lines: 70 },
  },
};
