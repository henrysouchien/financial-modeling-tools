import { findMatchInArray, scaleAndRound } from "./helpers";
import { vbaToNumber } from "./numeric";

export function matchConstant(
  value: number,
  searchArray: Array<number | string | null>,
  writeArray: Array<number | string | null>,
  collisionArray: number[],
  conversionFactor: number
): { found: boolean; newValue: number; isCollision: boolean; flipped: boolean; matchIndex: number } {
  const scaledValue = scaleAndRound(vbaToNumber(value), conversionFactor);
  const flippedScaledValue = scaleAndRound(-vbaToNumber(value), conversionFactor);

  const directMatch = findMatchInArray(scaledValue, searchArray, writeArray);
  if (directMatch) {
    const writeNum = vbaToNumber(writeArray[directMatch.index]);
    return {
      found: true,
      newValue: writeNum / conversionFactor,
      isCollision: collisionArray[directMatch.index] === 1,
      flipped: false,
      matchIndex: directMatch.index,
    };
  }

  const flippedMatch = findMatchInArray(flippedScaledValue, searchArray, writeArray);
  if (flippedMatch) {
    const writeNum = vbaToNumber(writeArray[flippedMatch.index]);
    return {
      found: true,
      newValue: -(writeNum / conversionFactor),
      isCollision: collisionArray[flippedMatch.index] === 1,
      flipped: true,
      matchIndex: flippedMatch.index,
    };
  }

  return { found: false, newValue: 0, isCollision: false, flipped: false, matchIndex: -1 };
}
