import { findMatchInArray, scaleAndRound } from "./helpers";
import { vbaIsNumeric, vbaToNumber } from "./numeric";
import { AutoDetectMeta, MatcherValue } from "./types";

export function autoDetectConversionFactor(
  sourceValues: MatcherValue[],
  searchArray: Array<number | string | null>,
  writeArray: Array<number | string | null>
): { factor: number | null; meta: AutoDetectMeta } {
  const candidates = [1, 1000, 1_000_000];
  let bestCandidate: number | null = null;
  let bestMatches = 0;
  const candidateResults: Record<number, number> = {};

  for (const factor of candidates) {
    let matches = 0;
    const usedIndices = new Set<number>();

    for (const cellVal of sourceValues) {
      if (!vbaIsNumeric(cellVal) || vbaToNumber(cellVal) === 0) continue;

      const numValue = vbaToNumber(cellVal);
      const scaledValue = scaleAndRound(numValue, factor);
      const flippedValue = scaleAndRound(-numValue, factor);

      let found = findMatchInArray(scaledValue, searchArray, writeArray, usedIndices);
      if (!found) {
        found = findMatchInArray(flippedValue, searchArray, writeArray, usedIndices);
      }

      if (found) {
        matches++;
        usedIndices.add(found.index);
      }
    }

    candidateResults[factor] = matches;
    if (matches > bestMatches) {
      bestMatches = matches;
      bestCandidate = factor;
    }
  }

  const validSamples = sourceValues.filter((value) => vbaIsNumeric(value) && vbaToNumber(value) !== 0).length;
  const minMatches = Math.max(3, Math.floor(validSamples * 0.05));
  const factor = bestMatches >= minMatches ? bestCandidate : null;
  return { factor, meta: { candidates: candidateResults, validSamples, minMatches } };
}
