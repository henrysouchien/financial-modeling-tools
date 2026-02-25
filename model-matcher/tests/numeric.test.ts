import assert from "node:assert/strict";
import test from "node:test";

import {
  normalizeNumericString,
  vbaIsNumeric,
  vbaRound,
  vbaToNumber,
  vbaVal,
  vbaValueEqualsZero,
} from "../src/numeric";

test("normalizeNumericString parses currency, commas, and parentheses", () => {
  assert.equal(normalizeNumericString("1,234"), 1234);
  assert.equal(normalizeNumericString("(1,234)"), -1234);
  assert.equal(normalizeNumericString("$2,500.50"), 2500.5);
  assert.equal(normalizeNumericString(","), null);
});

test("vbaVal extracts leading numeric substrings", () => {
  assert.equal(vbaVal("  -12.5abc"), -12.5);
  assert.equal(vbaVal("foo"), 0);
  assert.equal(vbaVal(null), 0);
});

test("vbaRound uses banker's rounding ties-to-even", () => {
  assert.equal(vbaRound(2.5, 0), 2);
  assert.equal(vbaRound(3.5, 0), 4);
  assert.equal(vbaRound(-2.5, 0), -2);
  assert.equal(vbaRound(-3.5, 0), -4);
});

test("numeric helpers align with update-model predicates", () => {
  assert.equal(vbaIsNumeric("(42)"), true);
  assert.equal(vbaToNumber("(42)"), -42);
  assert.equal(vbaValueEqualsZero("0"), true);
  assert.equal(vbaValueEqualsZero("foo"), false);
});
