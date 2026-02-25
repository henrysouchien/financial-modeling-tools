import assert from "node:assert/strict";
import test from "node:test";

import { match } from "../src";

test("parity fixture: constants, sign flip, formula replacement", () => {
  const result = match({
    data: [
      { prior_value: 100000, current_value: 120000, collision_flag: 0 },
      { prior_value: 50000, current_value: 60000, collision_flag: 0 },
      { prior_value: 200000, current_value: 240000, collision_flag: 0 },
    ],
    source_values: [100, -50, 200],
    source_formulas: [null, null, "=200/100-1"],
    conversion_factor: 1000,
  });

  assert.equal(result.cells_processed, 3);
  assert.equal(result.cells_updated, 3);
  assert.equal(result.cells_cleared, 0);
  assert.equal(result.collisions, 0);
  assert.equal(result.changes.length, 3);

  assert.deepEqual(
    result.changes.map((c) => c.change_type),
    ["constant", "constant", "formula"]
  );
  assert.equal(result.changes[0].new_value, 120);
  assert.equal(result.changes[1].new_value, -60);
  assert.equal(result.changes[2].new_formula, "=+240/+120-1");
});
