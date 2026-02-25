import { vbaIsNumeric, vbaRound, vbaToNumber } from "./numeric";

export function scaleAndRound(value: number, factor: number): number {
  return vbaRound(value * factor, 0);
}

export function findMatchInArray(
  scaledValue: number,
  searchArray: Array<number | string | null>,
  writeArray: Array<number | string | null>,
  excludedIndices?: Set<number>
): { index: number; flipped: boolean } | null {
  for (let j = 1; j < searchArray.length; j++) {
    if (excludedIndices?.has(j)) continue;
    if (!vbaIsNumeric(searchArray[j]) || !vbaIsNumeric(writeArray[j]) || vbaToNumber(writeArray[j]) === 0) {
      continue;
    }
    const roundedSearch = vbaRound(vbaToNumber(searchArray[j]), 0);
    if (roundedSearch === scaledValue) {
      return { index: j, flipped: false };
    }
  }
  return null;
}
