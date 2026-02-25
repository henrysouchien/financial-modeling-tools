# model-matcher

Pure TypeScript library for matching prior/current financial data into an existing model column using constant and formula matching.

## Install

```bash
npm install model-matcher
```

## Usage

```ts
import { match } from "model-matcher";

const result = match({
  data: [
    { prior_value: 100000, current_value: 120000, collision_flag: false },
    { prior_value: 200000, current_value: 240000, collision_flag: true },
  ],
  source_values: [100, 200],
  source_formulas: [null, "=200/100-1"],
  conversion_factor: 1000,
});
```

`result.changes` includes every row (`constant`, `formula`, `cleared`, `unchanged`, `skipped`) plus match statistics and conversion factor metadata.
