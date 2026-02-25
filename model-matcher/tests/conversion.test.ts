import assert from "node:assert/strict";
import test from "node:test";

import { autoDetectConversionFactor } from "../src/conversion";

test("autoDetectConversionFactor picks factor with strongest unique matches", () => {
  const sourceValues = [100, 200, 50];
  const searchArray = [null, null, 100000, 200000, 50000];
  const writeArray = [null, null, 120000, 240000, 60000];

  const result = autoDetectConversionFactor(sourceValues, searchArray, writeArray);
  assert.equal(result.factor, 1000);
  assert.equal(result.meta.candidates[1000], 3);
});

test("autoDetectConversionFactor returns null when below threshold", () => {
  const sourceValues = [999, 888, 777];
  const searchArray = [null, null, 100000, 200000];
  const writeArray = [null, null, 120000, 240000];

  const result = autoDetectConversionFactor(sourceValues, searchArray, writeArray);
  assert.equal(result.factor, null);
  assert.equal(result.meta.validSamples, 3);
  assert.equal(result.meta.minMatches, 3);
});

test("autoDetectConversionFactor enforces one-to-one matching", () => {
  const sourceValues = [100, 100, 100];
  const searchArray = [null, null, 100000];
  const writeArray = [null, null, 120000];

  const result = autoDetectConversionFactor(sourceValues, searchArray, writeArray);
  assert.equal(result.meta.candidates[1000], 1);
});
