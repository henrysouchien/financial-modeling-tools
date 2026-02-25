"""Minimal Excel formula parser used by the pattern matcher.

Purpose:
- Turn a raw Excel formula string into a small AST that is easy to pattern match.
- Support only the constructs we need for our financial-model patterns.

What it supports:
- Cell references with optional sheet prefix, absolute markers ($), and ranges.
- Numeric literals.
- Unary +/- and binary +, -, *, /, ^ with correct precedence and right-assoc power.
- Function calls with comma-separated args; empty args are allowed (e.g. OFFSET(...,)).

What it does not support:
- Full Excel formula semantics, named ranges, structured references, or array formulas.
- Evaluation. This parser only builds a syntax tree.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Node:
    pass


@dataclass
class Number(Node):
    value: float


@dataclass
class Ref(Node):
    sheet: Optional[str]
    col: str
    row: int


@dataclass
class Range(Node):
    start: Ref
    end: Ref


@dataclass
class UnaryOp(Node):
    op: str
    expr: Node


@dataclass
class BinaryOp(Node):
    op: str
    left: Node
    right: Node


@dataclass
class FuncCall(Node):
    name: str
    args: List[Node]


@dataclass
class Empty(Node):
    pass

class FormulaParseError(ValueError):
    pass


@dataclass
class Token:
    type: str
    value: str


class FormulaParser:
    """Parse an Excel formula string into a lightweight AST.

    Notes:
    - Supports basic arithmetic (+, -, *, /, ^) with precedence.
    - Supports refs (with optional sheet), ranges, and function calls.
    - Allows empty args in function calls (e.g., OFFSET(...,)).
    """
    def __init__(self, text: str) -> None:
        self.text = text.strip()
        if self.text.startswith("="):
            self.text = self.text[1:]
        self.pos = 0
        self.tokens = self._tokenize(self.text)
        self.index = 0

    def parse(self) -> Node:
        """Parse the full formula into an AST node.

        The parser consumes the entire token stream. Any trailing tokens
        indicate a syntax error and raise FormulaParseError.
        """
        if not self.tokens:
            raise FormulaParseError("Empty formula")
        node = self._parse_expression()
        if self._peek() is not None:
            raise FormulaParseError(f"Unexpected token: {self._peek()}")
        return node

    def _peek(self) -> Optional[Token]:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def _advance(self) -> Token:
        token = self._peek()
        if token is None:
            raise FormulaParseError("Unexpected end of formula")
        self.index += 1
        return token

    def _match(self, token_type: str, value: Optional[str] = None) -> bool:
        token = self._peek()
        if token is None:
            return False
        if token.type != token_type:
            return False
        if value is not None and token.value != value:
            return False
        self.index += 1
        return True

    def _parse_expression(self) -> Node:
        node = self._parse_term()
        while True:
            token = self._peek()
            if token and token.type == "OP" and token.value in {"+", "-"}:
                self._advance()
                right = self._parse_term()
                node = BinaryOp(token.value, node, right)
            else:
                break
        return node

    def _parse_term(self) -> Node:
        node = self._parse_power()
        while True:
            token = self._peek()
            if token and token.type == "OP" and token.value in {"*", "/"}:
                self._advance()
                right = self._parse_power()
                node = BinaryOp(token.value, node, right)
            else:
                break
        return node

    def _parse_power(self) -> Node:
        node = self._parse_unary()
        token = self._peek()
        if token and token.type == "OP" and token.value == "^":
            self._advance()
            right = self._parse_power()
            return BinaryOp("^", node, right)
        return node

    def _parse_unary(self) -> Node:
        token = self._peek()
        if token and token.type == "OP" and token.value in {"+", "-"}:
            self._advance()
            expr = self._parse_unary()
            return UnaryOp(token.value, expr)
        return self._parse_primary()

    def _parse_primary(self) -> Node:
        token = self._peek()
        if token is None:
            raise FormulaParseError("Unexpected end of formula")

        if token.type == "NUMBER":
            self._advance()
            return Number(float(token.value))

        if token.type == "REF":
            self._advance()
            ref = self._parse_ref(token.value)
            if self._match("OP", ":"):
                end_token = self._advance()
                if end_token.type != "REF":
                    raise FormulaParseError("Range end must be a cell reference")
                end_ref = self._parse_ref(end_token.value)
                return Range(ref, end_ref)
            return ref

        if token.type == "IDENT":
            self._advance()
            name = token.value.upper()
            if self._match("LPAREN"):
                args = []
                if not self._match("RPAREN"):
                    while True:
                        token = self._peek()
                        if token and token.type == "COMMA":
                            # Leading or consecutive empty arg
                            self._advance()
                            args.append(Empty())
                            if self._match("RPAREN"):
                                break
                            continue
                        if token and token.type == "RPAREN":
                            self._advance()
                            break

                        args.append(self._parse_expression())
                        if self._match("RPAREN"):
                            break
                        if self._match("COMMA"):
                            if self._peek() and self._peek().type == "RPAREN":
                                self._advance()
                                args.append(Empty())
                                break
                            continue
                        raise FormulaParseError("Expected ',' or ')' in function call")
                return FuncCall(name, args)
            return FuncCall(name, [])

        if self._match("LPAREN"):
            node = self._parse_expression()
            if not self._match("RPAREN"):
                raise FormulaParseError("Expected ')' after expression")
            return node

        raise FormulaParseError(f"Unexpected token: {token}")

    def _parse_ref(self, raw: str) -> Ref:
        if "!" in raw:
            sheet, cell = raw.split("!", 1)
        else:
            sheet, cell = None, raw
        sheet = sheet.strip("'") if sheet else None
        col, row = self._split_cell(cell)
        return Ref(sheet=sheet, col=col, row=row)

    def _split_cell(self, cell: str) -> Tuple[str, int]:
        cell = cell.replace("$", "")
        col = ""
        row = ""
        for ch in cell:
            if ch.isalpha():
                col += ch.upper()
            else:
                row += ch
        if not col or not row:
            raise FormulaParseError(f"Invalid cell reference: {cell}")
        return col, int(row)

    def _tokenize(self, text: str) -> List[Token]:
        tokens: List[Token] = []
        i = 0
        length = len(text)
        while i < length:
            ch = text[i]
            if ch.isspace():
                i += 1
                continue

            if ch in "+-*/^:":
                tokens.append(Token("OP", ch))
                i += 1
                continue

            if ch == "(":
                tokens.append(Token("LPAREN", ch))
                i += 1
                continue

            if ch == ")":
                tokens.append(Token("RPAREN", ch))
                i += 1
                continue

            if ch == ",":
                tokens.append(Token("COMMA", ch))
                i += 1
                continue

            if ch == "'":
                end = text.find("'", i + 1)
                if end == -1:
                    raise FormulaParseError("Unterminated sheet name")
                sheet = text[i + 1 : end]
                if end + 1 < length and text[end + 1] == "!":
                    ref_start = end + 2
                    cell, ref_len = self._read_cell(text[ref_start:])
                    tokens.append(Token("REF", f"'{sheet}'!{cell}"))
                    i = ref_start + ref_len
                    continue
                tokens.append(Token("IDENT", sheet))
                i = end + 1
                continue

            if ch.isdigit() or (ch == "." and i + 1 < length and text[i + 1].isdigit()):
                number, consumed = self._read_number(text[i:])
                tokens.append(Token("NUMBER", number))
                i += consumed
                continue

            if ch.isalpha() or ch == "$":
                # try sheet!cell
                sheet_cell, consumed = self._read_sheet_cell(text[i:])
                if sheet_cell:
                    tokens.append(Token("REF", sheet_cell))
                    i += consumed
                    continue

                cell, consumed = self._read_cell(text[i:])
                if cell:
                    tokens.append(Token("REF", cell))
                    i += consumed
                    continue

                ident, consumed = self._read_ident(text[i:])
                tokens.append(Token("IDENT", ident))
                i += consumed
                continue

            raise FormulaParseError(f"Unexpected character: {ch}")
        return tokens

    def _read_number(self, text: str) -> Tuple[str, int]:
        i = 0
        has_dot = False
        while i < len(text):
            ch = text[i]
            if ch.isdigit():
                i += 1
                continue
            if ch == "." and not has_dot:
                has_dot = True
                i += 1
                continue
            break
        return text[:i], i

    def _read_ident(self, text: str) -> Tuple[str, int]:
        i = 0
        while i < len(text) and (text[i].isalnum() or text[i] in {"_", "."}):
            i += 1
        return text[:i], i

    def _read_cell(self, text: str) -> Tuple[Optional[str], int]:
        i = 0
        cell = ""
        while i < len(text) and text[i] == "$":
            cell += text[i]
            i += 1
        while i < len(text) and text[i].isalpha():
            cell += text[i]
            i += 1
        if not cell or cell == "$":
            return None, 0
        while i < len(text) and text[i] == "$":
            cell += text[i]
            i += 1
        digit_start = i
        while i < len(text) and text[i].isdigit():
            cell += text[i]
            i += 1
        if i == digit_start:
            return None, 0
        return cell, i

    def _read_sheet_cell(self, text: str) -> Tuple[Optional[str], int]:
        # Only support unquoted sheet names without spaces.
        i = 0
        while i < len(text) and (text[i].isalnum() or text[i] in {"_", "."}):
            i += 1
        if i == 0 or i >= len(text) or text[i] != "!":
            return None, 0
        sheet = text[:i]
        cell, consumed = self._read_cell(text[i + 1 :])
        if not cell:
            return None, 0
        return f"{sheet}!{cell}", i + 1 + consumed
