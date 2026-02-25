import assert from "node:assert/strict";
import test from "node:test";

import { matchConstant } from "../src/constants";

test("matchConstant matches direct values and propagates collisions", () => {
  const searchArray = [null, null, 100000];
  const writeArray = [null, null, 120000];
  const collisionArray = [0, 0, 1];

  const result = matchConstant(100, searchArray, writeArray, collisionArray, 1000);
  assert.equal(result.found, true);
  assert.equal(result.newValue, 120);
  assert.equal(result.isCollision, true);
  assert.equal(result.flipped, false);
});

test("matchConstant supports sign-flip matches", () => {
  const searchArray = [null, null, 50000];
  const writeArray = [null, null, 60000];
  const collisionArray = [0, 0, 0];

  const result = matchConstant(-50, searchArray, writeArray, collisionArray, 1000);
  assert.equal(result.found, true);
  assert.equal(result.newValue, -60);
  assert.equal(result.flipped, true);
});

test("matchConstant prioritizes direct over sign-flip", () => {
  const searchArray = [null, null, 100000, -100000];
  const writeArray = [null, null, 110000, 220000];
  const collisionArray = [0, 0, 0, 0];

  const result = matchConstant(100, searchArray, writeArray, collisionArray, 1000);
  assert.equal(result.found, true);
  assert.equal(result.newValue, 110);
  assert.equal(result.matchIndex, 2);
});
