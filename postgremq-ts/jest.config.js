/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/__tests__/**/*.test.ts'],
  transform: {
    '^.+\\.ts$': ['ts-jest', {
      tsconfig: {
        esModuleInterop: true
      }
    }]
  },
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
    '!src/__tests__/**',
    '!src/index.ts',
    '!src/migrations.generated.ts'
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  coverageThreshold: {
    global: {
      branches: 63,
      functions: 75,
      lines: 78,
      statements: 78
    }
  },
  testTimeout: 120000, // 120 seconds to allow container start/pulls
  // detectOpenHandles is intentionally OFF: it implies --runInBand (serial),
  // which defeats maxWorkers. Re-enable it temporarily (or run
  // `jest --detectOpenHandles`) only when debugging a leaked handle.
  forceExit: false,
  // Generates src/migrations.generated.ts (see scripts/embed-migrations.js).
  globalSetup: '<rootDir>/scripts/embed-migrations.js',
  setupFilesAfterEnv: ['<rootDir>/src/__tests__/setup.ts'],
  // Each test runs in its own freshly-created database (createIsolatedTestConnection),
  // and each worker process reuses a single shared container (getSharedTestDatabase),
  // so tests are fully isolated and safe to run in parallel. Mirrors Go's model
  // (one shared container + parallel tests). Workers run test FILES concurrently,
  // so the per-file real-time waits (vt expiry, keep-alive) overlap instead of summing.
  maxWorkers: '50%'
};
