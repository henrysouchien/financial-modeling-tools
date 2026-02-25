import { vbaIsNumeric, vbaRound, vbaToNumber } from "./numeric";

const DEBUG_MATCH_FORMULA = false;

export function tokenizeFormula(formula: string): string[] {
  if (formula.startsWith("=")) {
    formula = formula.substring(1);
  }

  const tokens: string[] = [];
  let token = "";
  let pos = 0;

  const isNotCellRefChar = (ch: string): boolean => {
    return !/[\$0-9A-Z]/.test(ch);
  };

  const isNotCellRefOrDecimal = (ch: string): boolean => {
    return !/[\$0-9A-Z\.]/.test(ch);
  };

  while (pos < formula.length) {
    const c = formula[pos];

    if (c === "+" || c === "-") {
      if (token !== "") {
        tokens.push(token);
        token = "";
      }

      if (pos + 1 < formula.length) {
        const nextChar = formula[pos + 1];
        if (isNotCellRefChar(nextChar)) {
          token = c;
          pos++;
          while (pos < formula.length && isNotCellRefOrDecimal(formula[pos])) {
            token += formula[pos];
            pos++;
          }
          tokens.push(token);
          token = "";
          continue;
        }
      }

      tokens.push(c);
    } else if (c === "*" || c === "/" || c === "(" || c === ")" || c === "=" || c === ",") {
      if (token !== "") {
        tokens.push(token);
        token = "";
      }
      tokens.push(c);
    } else {
      token += c;
    }

    pos++;
  }

  if (token !== "") {
    tokens.push(token);
  } else if (tokens.length > 0 && tokens[tokens.length - 1] === "") {
    tokens.pop();
  }

  if (tokens.length === 0) return [""];
  return tokens;
}

export function rebuildFormula(tokens: string[]): string {
  return "=" + tokens.join("");
}

export function vbaMatch(lookupValue: number, searchArray: Array<number | string | null>): number {
  for (let j = 1; j < searchArray.length; j++) {
    const candidate = searchArray[j];
    if (vbaIsNumeric(candidate)) {
      if (vbaToNumber(candidate) === lookupValue) return j;
    } else if (candidate === lookupValue) {
      return j;
    }
  }
  return -1;
}

export function formatForFormula(value: number): string {
  const rounded = vbaRound(value, 3);
  const fixed = rounded.toFixed(3);
  const trimmed = fixed.replace(/\.?0+$/, "");
  if (rounded >= 0) {
    return "+" + trimmed;
  }
  return trimmed;
}

export function matchFormula(
  formula: string,
  searchArray: Array<number | string | null>,
  writeArray: Array<number | string | null>,
  collisionArray: number[],
  conversionFactor: number
): { formula: string; collisionHits: number; matchHits: number } {
  const tokens = tokenizeFormula(formula);
  let collisionHits = 0;
  let matchHits = 0;

  if (DEBUG_MATCH_FORMULA) {
    console.log(`[matchFormula] input: "${formula}" -> tokens: [${tokens.map((t) => JSON.stringify(t)).join(", ")}]`);
  }

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];

    if (!vbaIsNumeric(token)) continue;
    if (["0", "1", "-1"].includes(token)) continue;

    const numValue = vbaToNumber(token);
    const scaledValue = numValue * conversionFactor;
    const roundedScaledValue = vbaRound(scaledValue, 0);

    let matchIndex = vbaMatch(roundedScaledValue, searchArray);
    let flipped = false;

    if (matchIndex === -1) {
      const flippedRoundedValue = vbaRound(-scaledValue, 0);
      matchIndex = vbaMatch(flippedRoundedValue, searchArray);
      flipped = matchIndex !== -1;
    }

    if (matchIndex !== -1) {
      matchHits++;
      const rawWrite = vbaToNumber(writeArray[matchIndex]);
      const baseValue = isNaN(rawWrite) ? 0 : rawWrite / conversionFactor;
      const newValue = flipped ? -baseValue : baseValue;
      if (DEBUG_MATCH_FORMULA) {
        console.log(
          `[matchFormula]   token[${i}] ${JSON.stringify(token)} -> matched[${matchIndex}] -> ${formatForFormula(newValue)}${flipped ? " (flipped)" : ""}`
        );
      }
      tokens[i] = formatForFormula(newValue);
      if (collisionArray[matchIndex] === 1) collisionHits++;
    } else {
      if (DEBUG_MATCH_FORMULA) {
        console.log(`[matchFormula]   token[${i}] ${JSON.stringify(token)} -> no match -> "0"`);
      }
      tokens[i] = "0";
    }
  }

  const rebuilt = rebuildFormula(tokens);
  if (DEBUG_MATCH_FORMULA && rebuilt !== formula) {
    console.log(`[matchFormula] CHANGED: "${formula}" -> "${rebuilt}"`);
  }

  return {
    formula: rebuilt,
    collisionHits,
    matchHits,
  };
}
