/**
 * Test Setup
 * Runs before all tests to configure the test environment
 */

import {stopSharedTestDatabase} from './helpers';
import { jest, beforeAll, afterAll } from '@jest/globals';

// Increase test timeout for integration tests with Docker
jest.setTimeout(120000);

// Suppress console output during tests (optional)
// Uncomment to reduce noise in test output
// global.console = {
//   ...console,
//   log: jest.fn(),
//   debug: jest.fn(),
//   info: jest.fn(),
//   warn: jest.fn(),
// };

// Global test setup
beforeAll(async () => {
  // Any global setup logic here
});

// Global test teardown
afterAll(async () => {
  await stopSharedTestDatabase();
});
