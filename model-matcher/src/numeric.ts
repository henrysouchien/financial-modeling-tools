export function normalizeNumericString(value: string): number | null {
  const trimmed = value.trim();
  if (trimmed === "") return null;
  const paren = trimmed.match(/^\((.*)\)$/);
  const raw = paren ? `-${paren[1]}` : trimmed;
  const cleaned = raw.replace(/[$£€¥,]/g, "");
  if (cleaned === "") return null;
  const num = Number(cleaned);
  return isNaN(num) ? null : num;
}

export function vbaVal(value: unknown): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "number") return isNaN(value) ? 0 : value;
  const str = String(value).trimStart();
  const match = str.match(/^[+-]?(?:\d+(\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/);
  return match ? Number(match[0]) : 0;
}

export function vbaIsNumeric(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "number") return !isNaN(value);
  if (typeof value === "string") {
    const normalized = normalizeNumericString(value);
    return normalized !== null;
  }
  return false;
}

export function vbaToNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const normalized = normalizeNumericString(value);
    return normalized === null ? NaN : normalized;
  }
  return NaN;
}

export function vbaValueEqualsZero(value: unknown): boolean {
  return vbaIsNumeric(value) && vbaToNumber(value) === 0;
}

export function vbaRound(value: number, digits = 0): number {
  const factor = Math.pow(10, digits);
  const scaled = value * factor;
  if (!isFinite(scaled)) return value;

  const sign = scaled < 0 ? -1 : 1;
  const abs = Math.abs(scaled);
  const floor = Math.floor(abs);
  const frac = abs - floor;
  const epsilon = 1e-12;

  let rounded: number;
  if (frac > 0.5 + epsilon) {
    rounded = floor + 1;
  } else if (frac < 0.5 - epsilon) {
    rounded = floor;
  } else {
    rounded = floor % 2 === 0 ? floor : floor + 1;
  }

  return (sign * rounded) / factor;
}
