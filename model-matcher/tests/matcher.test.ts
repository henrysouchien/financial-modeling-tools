import assert from "node:assert/strict";
import test from "node:test";

import { match, MatcherError } from "../src";

test("match returns full-row envelope with skipped and actionable changes", () => {
  const result = match({
    data: [
      { prior_value: 100000, current_value: 120000, collision_flag: 1 },
      { prior_value: 200000, current_value: 240000, collision_flag: 0 },
      { prior_value: 300000, current_value: 360000, collision_flag: 0 },
    ],
    source_values: [100, 200, "", 0, "#N/A", "text"],
    source_formulas: [null, "=200/100-1", null, null, null, null],
    conversion_factor: 1000,
  });

  assert.equal(result.changes.length, 6);
  assert.equal(result.cells_processed, 2);
  assert.equal(result.cells_updated, 2);
  assert.equal(result.cells_cleared, 0);
  assert.equal(result.collisions, 2);
  assert.equal(result.conversion_factor, 1000);
  assert.equal(result.conversion_factor_source, "provided");

  assert.equal(result.changes[0].change_type, "constant");
  assert.equal(result.changes[0].new_value, 120);
  assert.equal(result.changes[0].is_collision, true);

  assert.equal(result.changes[1].change_type, "formula");
  assert.equal(result.changes[1].new_formula, "=+240/+120-1");
  assert.equal(result.changes[1].collision_hits, 1);

  assert.equal(result.changes[2].change_type, "skipped");
  assert.equal(result.changes[2].skip_reason, "empty");
  assert.equal(result.changes[3].skip_reason, "zero");
  assert.equal(result.changes[4].skip_reason, "error");
  assert.equal(result.changes[5].skip_reason, "non_numeric");
});

test("match validates source_formulas alignment", () => {
  assert.throws(
    () =>
      match({
        data: [],
        source_values: [1],
        source_formulas: [],
        conversion_factor: 1,
      }),
    (err: unknown) => err instanceof MatcherError && err.code === "source_formula_mismatch"
  );
});

test("match throws conversion_factor_required when auto-detect fails", () => {
  assert.throws(
    () =>
      match({
        data: [{ prior_value: 100000, current_value: 120000 }],
        source_values: [999],
      }),
    (err: unknown) => err instanceof MatcherError && err.code === "conversion_factor_required"
  );
});
