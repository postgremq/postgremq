import { describe, test, expect } from '@jest/globals';
import { mqLine } from '../compatibility';

describe('mqLine', () => {
  test('is the major, or before 1.0 the major and minor', () => {
    const cases: Array<[number, string]> = [
      [1, '0.0'],
      [10_000, '0.1'],
      [19_999, '0.1'],
      [20_000, '0.2'],
      [99_999_999, '0.9999'],
      [100_000_000, '1'],
      [100_020_003, '1'],
      [299_990_000, '2'],
    ];
    for (const [version, line] of cases) {
      expect(mqLine(version)).toBe(line);
    }
  });
});
