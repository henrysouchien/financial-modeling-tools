export type MatcherValue = number | string | null;

export interface MatcherDataRow {
  current_value: MatcherValue;
  prior_value: MatcherValue;
  collision_flag?: boolean | number | string | null;
}

export interface MatcherInput {
  data: MatcherDataRow[];
  source_values: MatcherValue[];
  source_formulas?: Array<string | null>;
  conversion_factor?: number;
  reverse_mode?: boolean;
}

export type MatcherChangeType = "constant" | "formula" | "cleared" | "unchanged" | "skipped";
export type MatcherSkipReason = "error" | "empty" | "zero" | "non_numeric";

export interface MatcherChange {
  index: number;
  current_value: MatcherValue;
  new_value: MatcherValue;
  change_type: MatcherChangeType;
  is_collision: boolean;
  skip_reason?: MatcherSkipReason;
  new_formula?: string;
  match_hits?: number;
  collision_hits?: number;
}

export interface AutoDetectMeta {
  candidates: Record<number, number>;
  validSamples: number;
  minMatches: number;
}

export interface MatcherResult {
  changes: MatcherChange[];
  conversion_factor: number;
  conversion_factor_source: "provided" | "auto-detected";
  auto_detect_meta?: AutoDetectMeta;
  cells_processed: number;
  cells_updated: number;
  cells_cleared: number;
  collisions: number;
  collision_rate: number;
  low_match_warning?: string;
}

export type MatcherErrorCode =
  | "source_formula_mismatch"
  | "invalid_conversion_factor"
  | "conversion_factor_required";

export class MatcherError extends Error {
  code: MatcherErrorCode;
  details?: Record<string, unknown>;

  constructor(code: MatcherErrorCode, message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = "MatcherError";
    this.code = code;
    this.details = details;
  }
}
