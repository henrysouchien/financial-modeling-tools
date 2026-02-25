import assert from "node:assert/strict";
import test from "node:test";

import { matchFormula, tokenizeFormula } from "../src/formulas";

test("tokenizeFormula keeps commas as delimiters", () => {
  const tokens = tokenizeFormula("=IF((X-Y)>0,(X-Y),0)");
  assert.deepEqual(tokens, ["IF", "(", "(", "X", "-", "Y", ")", ">0", ",", "(", "X", "-", "Y", ")", ",", "0", ")"]);
});

test("matchFormula replaces numeric literals, leaves refs", () => {
  const searchArray = [null, null, 200000, 100000];
  const writeArray = [null, null, 240000, 110000];
  const collisionArray = [0, 0, 0, 1];

  const result = matchFormula("=200/100-1+A5", searchArray, writeArray, collisionArray, 1000);
  assert.equal(result.formula, "=+240/+110-1+A5");
  assert.equal(result.matchHits, 2);
  assert.equal(result.collisionHits, 1);
});

test("matchFormula zeros unmatched numeric tokens", () => {
  const searchArray = [null, null, 200000];
  const writeArray = [null, null, 240000];
  const collisionArray = [0, 0, 0];

  const result = matchFormula("=200/999", searchArray, writeArray, collisionArray, 1000);
  assert.equal(result.formula, "=+240/0");
  assert.equal(result.matchHits, 1);
});
