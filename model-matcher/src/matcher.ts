import { matchConstant } from "./constants";
import { autoDetectConversionFactor } from "./conversion";
import { matchFormula } from "./formulas";
import { vbaIsNumeric, vbaToNumber, vbaVal, vbaValueEqualsZero } from "./numeric";
import {
  MatcherChange,
  MatcherError,
  MatcherInput,
  MatcherResult,
  MatcherSkipReason,
  MatcherValue,
} from "./types";

function buildSkippedChange(
  index: number,
  currentValue: MatcherValue,
  reason: MatcherSkipReason
): MatcherChange {
  return {
    index,
    current_value: currentValue,
    new_value: currentValue,
    change_type: "skipped",
    is_collision: false,
    skip_reason: reason,
  };
}

export function match(input: MatcherInput): MatcherResult {
  const sourceValues = input.source_values || [];
  const sourceFormulas = input.source_formulas;
  if (sourceFormulas && sourceFormulas.length !== sourceValues.length) {
    throw new MatcherError(
      "source_formula_mismatch",
      "source_formulas must be the same length as source_values.",
      { source_values: sourceValues.length, source_formulas: sourceFormulas.length }
    );
  }

  const reverseMode = Boolean(input.reverse_mode);

  const currentArray: MatcherValue[] = [null];
  const priorArray: MatcherValue[] = [null];
  const collisionArray: number[] = [0];

  for (let i = 0; i < input.data.length; i++) {
    const rowIndex = i + 2;
    const row = input.data[i];
    currentArray[rowIndex] = row.current_value ?? null;
    priorArray[rowIndex] = row.prior_value ?? null;
    collisionArray[rowIndex] = vbaVal(row.collision_flag) === 1 ? 1 : 0;
  }

  const searchArray = reverseMode ? currentArray : priorArray;
  const writeArray = reverseMode ? priorArray : currentArray;

  let conversionFactor: number;
  let conversionFactorSource: "provided" | "auto-detected";
  let autoDetectMeta: MatcherResult["auto_detect_meta"];

  if (input.conversion_factor !== undefined && input.conversion_factor !== null) {
    conversionFactor = Number(input.conversion_factor);
    conversionFactorSource = "provided";
  } else {
    const detectResult = autoDetectConversionFactor(sourceValues, searchArray, writeArray);
    conversionFactor = detectResult.factor as number;
    conversionFactorSource = "auto-detected";
    autoDetectMeta = detectResult.meta;
    if (detectResult.factor === null) {
      throw new MatcherError(
        "conversion_factor_required",
        "Could not auto-detect conversion factor from source range values. Re-call update_model with an explicit conversion_factor parameter (1000 if model is in thousands, 1000000 if in millions, 1 if raw values).",
        { auto_detect: detectResult.meta }
      );
    }
  }

  if (!isFinite(conversionFactor) || conversionFactor === 0) {
    throw new MatcherError(
      "invalid_conversion_factor",
      "Conversion factor is 0 or invalid. Cannot proceed.",
      { conversion_factor: conversionFactor }
    );
  }

  const changes: MatcherChange[] = [];
  let processedCount = 0;
  let updatedCount = 0;
  let clearedCount = 0;
  let collisionCount = 0;

  for (let i = 0; i < sourceValues.length; i++) {
    const cellValue = sourceValues[i];
    const cellFormula = sourceFormulas ? sourceFormulas[i] : null;
    const hasFormula = typeof cellFormula === "string" && cellFormula.startsWith("=");

    const isError = typeof cellValue === "string" && cellValue.startsWith("#");
    const isEmpty = cellValue === "" || cellValue === null;
    if (isError) {
      changes.push(buildSkippedChange(i, hasFormula ? cellFormula : cellValue, "error"));
      continue;
    }
    if (isEmpty) {
      changes.push(buildSkippedChange(i, hasFormula ? cellFormula : cellValue, "empty"));
      continue;
    }
    if (vbaValueEqualsZero(cellValue)) {
      changes.push(buildSkippedChange(i, hasFormula ? cellFormula : cellValue, "zero"));
      continue;
    }
    if (!vbaIsNumeric(cellValue)) {
      changes.push(buildSkippedChange(i, hasFormula ? cellFormula : cellValue, "non_numeric"));
      continue;
    }

    processedCount++;

    if (!hasFormula) {
      const constantMatch = matchConstant(
        vbaToNumber(cellValue),
        searchArray,
        writeArray,
        collisionArray,
        conversionFactor
      );
      if (constantMatch.found) {
        updatedCount++;
        if (constantMatch.isCollision) {
          collisionCount++;
        }
        const unchanged = vbaIsNumeric(cellValue) && vbaToNumber(cellValue) === constantMatch.newValue;
        changes.push({
          index: i,
          current_value: cellValue,
          new_value: constantMatch.newValue,
          change_type: unchanged ? "unchanged" : "constant",
          is_collision: constantMatch.isCollision,
        });
      } else {
        clearedCount++;
        changes.push({
          index: i,
          current_value: cellValue,
          new_value: null,
          change_type: "cleared",
          is_collision: false,
        });
      }
      continue;
    }

    const matchedFormula = matchFormula(cellFormula, searchArray, writeArray, collisionArray, conversionFactor);
    if (matchedFormula.matchHits > 0) {
      updatedCount++;
    }
    if (matchedFormula.collisionHits > 0) {
      collisionCount += matchedFormula.collisionHits;
    }

    changes.push({
      index: i,
      current_value: cellFormula,
      new_value: matchedFormula.formula,
      change_type: matchedFormula.formula === cellFormula ? "unchanged" : "formula",
      is_collision: matchedFormula.collisionHits > 0,
      new_formula: matchedFormula.formula,
      match_hits: matchedFormula.matchHits,
      collision_hits: matchedFormula.collisionHits,
    });
  }

  const collisionFacts = input.data.filter((row) => vbaVal(row.collision_flag) === 1).length;
  const collisionRate = input.data.length > 0 ? collisionFacts / input.data.length : 0;

  const updateRate = processedCount > 0 ? updatedCount / processedCount : 0;
  const lowMatch = updateRate < 0.15 && processedCount > 20;
  const lowMatchWarning = lowMatch
    ? `Low match rate: only ${updatedCount} of ${processedCount} cells matched (${(updateRate * 100).toFixed(0)}%). This may indicate wrong parameters (e.g., full_year_mode, wrong source column, or wrong period). Check the target column to verify key line items (revenue, net income, etc.) were populated.`
    : undefined;

  return {
    changes,
    conversion_factor: conversionFactor,
    conversion_factor_source: conversionFactorSource,
    auto_detect_meta: autoDetectMeta,
    cells_processed: processedCount,
    cells_updated: updatedCount,
    cells_cleared: clearedCount,
    collisions: collisionCount,
    collision_rate: collisionRate,
    low_match_warning: lowMatchWarning,
  };
}
