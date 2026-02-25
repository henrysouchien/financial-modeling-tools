export { autoDetectConversionFactor } from "./conversion";
export { matchConstant } from "./constants";
export { formatForFormula, matchFormula, rebuildFormula, tokenizeFormula, vbaMatch } from "./formulas";
export {
  normalizeNumericString,
  vbaIsNumeric,
  vbaRound,
  vbaToNumber,
  vbaVal,
  vbaValueEqualsZero,
} from "./numeric";
export { match } from "./matcher";
export { MatcherError } from "./types";
export type {
  AutoDetectMeta,
  MatcherChange,
  MatcherChangeType,
  MatcherDataRow,
  MatcherErrorCode,
  MatcherInput,
  MatcherResult,
  MatcherSkipReason,
  MatcherValue,
} from "./types";
