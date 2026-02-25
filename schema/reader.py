"""Excel → Schema reader utilities.

Purpose:
- Parse .xlsx files and build a minimal FinancialModel.
- Extract labels, formulas, values, and time structure from sheet XML.
- Classify formulas using FormulaPatternMatcher and attach FormulaSpecs.

The reader is intentionally heuristic:
- Year headers are inferred from the densest row of year-like values.
- Column A labels are slugified into line_item_ids.
- Shared formulas (Excel's shared formula optimization) are not expanded; cached values are used.
- Named ranges and structured references are not resolved.

Example:
Row label: "Gross Profit" (row 9) → line_item_id: "gross_profit"
Cell H9 formula: =Assumptions!H34 → FormulaSpec(type=ref, source=LineItemRef("gross_profit", t=0))
"""

from __future__ import annotations

import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from .models import (
    CompanyInfo,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ModelMetadata,
    ScenarioInputs,
    Section,
    Sheet,
    TimeStructure,
    Unit,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    PERIOD_MODE_QUARTERLY5,
    PERIOD_MODE_YEARLY,
    encode_period,
    period_year,
)
from .pattern_matcher import CellContext, FormulaPatternMatcher


@dataclass
class CellData:
    value: Optional[str]
    formula: Optional[str]


class ExcelWorkbookReader:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def read(self) -> Dict[str, Dict[Tuple[int, int], CellData]]:
        """Load an .xlsx into a dict of sheet -> {(row, col): CellData}."""
        with zipfile.ZipFile(self.file_path) as zf:
            shared_strings = _load_shared_strings(zf)
            sheets = _load_sheets(zf)

            data: Dict[str, Dict[Tuple[int, int], CellData]] = {}
            for sheet_name, sheet_path in sheets.items():
                xml_data = zf.read(sheet_path)
                data[sheet_name] = _parse_sheet(xml_data, shared_strings)
        return data


def read_model(
    file_path: str,
    mode: str = "quick",
    historical_cutoff_year: Optional[int] = None,
    quarterly_mode: str = "auto",
) -> Dict:
    """Parse an Excel model into a schema (quick summary or full model)."""
    workbook = ExcelWorkbookReader(file_path).read()
    matcher = FormulaPatternMatcher()

    if quarterly_mode not in {"legacy", "auto", "quarterly_native"}:
        raise ValueError("quarterly_mode must be one of: legacy, auto, quarterly_native")

    workbook_has_quarterly = any(_sheet_has_quarterly_tokens(cells) for cells in workbook.values())
    if quarterly_mode == "legacy":
        period_mode = PERIOD_MODE_YEARLY
    elif quarterly_mode == "quarterly_native":
        period_mode = PERIOD_MODE_QUARTERLY5
    else:
        period_mode = PERIOD_MODE_QUARTERLY5 if workbook_has_quarterly else PERIOD_MODE_YEARLY

    sheet_row_to_item: Dict[str, Dict[int, str]] = {}
    sheet_col_to_period: Dict[str, Dict[int, int]] = {}
    sheet_quarterly_cols: Dict[str, Set[int]] = {}
    sheet_annual_cols: Dict[str, Set[int]] = {}
    period_set: Set[int] = set()

    for sheet_name, cells in workbook.items():
        row_to_item, label_rows = _extract_line_items(sheet_name, cells)
        sheet_row_to_item[sheet_name] = row_to_item
        col_to_period, quarterly_cols, annual_cols = _find_period_header(cells, period_mode)
        sheet_col_to_period[sheet_name] = col_to_period
        sheet_quarterly_cols[sheet_name] = quarterly_cols
        sheet_annual_cols[sheet_name] = annual_cols
        period_set.update(col_to_period.values())

    # Detect blank spacer rows referenced by formulas but not yet mapped
    for sheet_name, cells in workbook.items():
        _map_referenced_blank_rows(sheet_name, cells, sheet_row_to_item)

    time_order = sorted(period_set)

    formulas_by_row: Dict[Tuple[str, int], Dict[int, FormulaSpec]] = {}
    values_by_row: Dict[Tuple[str, int], ValueSeries] = {}
    classification_counts: Dict[str, int] = {}

    for sheet_name, cells in workbook.items():
        rows = sheet_row_to_item.get(sheet_name, {})
        for row, line_item_id in rows.items():
            values = ValueSeries()
            quarterly_cols = sheet_quarterly_cols.get(sheet_name, set())
            period_formula_list: Dict[int, List[FormulaSpec]] = {}
            period_quarterly_formulas: Dict[int, List[FormulaSpec]] = {}
            period_has_annual_value: Dict[int, bool] = {}
            for col_idx, period in sorted(sheet_col_to_period.get(sheet_name, {}).items()):
                cell = cells.get((row, col_idx))
                if cell is None:
                    continue
                has_value = cell.value not in (None, "")
                is_quarterly_col = col_idx in quarterly_cols
                if cell.formula:
                    context = CellContext(
                        sheet=sheet_name,
                        row=row,
                        col=col_idx,
                        sheet_row_to_item=sheet_row_to_item,
                        sheet_col_to_period=sheet_col_to_period,
                        time_order=time_order,
                    )
                    spec = matcher.classify(cell.formula, context)
                    classification_counts[spec.type.value] = classification_counts.get(spec.type.value, 0) + 1
                    if is_quarterly_col and quarterly_cols:
                        period_quarterly_formulas.setdefault(period, []).append(spec)
                    else:
                        period_formula_list.setdefault(period, []).append(spec)
                if has_value:
                    value = _coerce_number(cell.value)
                    if value is not None:
                        if period_mode == PERIOD_MODE_QUARTERLY5:
                            provenance = (
                                ValueProvenance.computed if cell.formula else ValueProvenance.imported_other
                            )
                            values.values[period] = ValueCell(
                                period=period,
                                value=value,
                                provenance=provenance,
                            )
                        elif (
                            period not in values.values
                            or not is_quarterly_col
                            or not period_has_annual_value.get(period)
                        ):
                            provenance = (
                                ValueProvenance.computed if cell.formula else ValueProvenance.imported_other
                            )
                            values.values[period] = ValueCell(
                                period=period,
                                value=value,
                                provenance=provenance,
                            )
                            if not is_quarterly_col:
                                period_has_annual_value[period] = True

            formulas_by_period: Dict[int, FormulaSpec] = {}
            all_formula_periods = set(period_formula_list) | set(period_quarterly_formulas)
            for period in all_formula_periods:
                specs = period_formula_list.get(period) or period_quarterly_formulas.get(period, [])
                if not specs:
                    continue
                sig_counts: Dict[Tuple[FormulaType, Optional[str], object], int] = {}
                sig_to_spec: Dict[Tuple[FormulaType, Optional[str], object], FormulaSpec] = {}
                for spec in specs:
                    sig = (spec.type, spec.subtype, _param_shape(spec.params))
                    sig_counts[sig] = sig_counts.get(sig, 0) + 1
                    sig_to_spec.setdefault(sig, spec)
                best_sig = max(sig_counts, key=sig_counts.get)
                spec = sig_to_spec[best_sig]
                if spec.type == FormulaType.raw:
                    cached = values.values.get(period) if values else None
                    if cached and cached.value is not None:
                        spec = FormulaSpec(
                            type=FormulaType.constant,
                            params={"value": cached.value},
                            note="raw_cached_fallback",
                        )
                if _is_self_referencing(spec, line_item_id):
                    cached = values.values.get(period) if values else None
                    if cached and cached.value is not None:
                        spec = FormulaSpec(
                            type=FormulaType.constant,
                            params={"value": cached.value},
                        )
                    else:
                        spec = FormulaSpec(
                            type=FormulaType.constant,
                            params={"value": 0.0},
                            note="self_ref_no_cache",
                        )
                if quarterly_cols:
                    spec = _dedup_additive_refs(spec)
                formulas_by_period[period] = spec

            if formulas_by_period:
                formulas_by_row[(sheet_name, row)] = formulas_by_period
            if values.values:
                values_by_row[(sheet_name, row)] = values

    time_structure = _build_time_structure(
        time_order,
        period_mode=period_mode,
        historical_cutoff_year=historical_cutoff_year,
    )

    model = FinancialModel(
        company=CompanyInfo(ticker="", name=""),
        time_structure=time_structure,
        sheets={},
        scenarios={},
        metadata=ModelMetadata(template_version=None),
    )

    # Detect label columns per sheet for label extraction
    sheet_label_col: Dict[str, int] = {}
    for sheet_name, cells in workbook.items():
        sheet_label_col[sheet_name] = _detect_label_column(cells)

    historical_periods = list(time_structure.historical_periods) or list(time_structure.historical_years)
    projection_periods = list(time_structure.projection_periods) or list(time_structure.projection_years)
    all_periods = set(historical_periods + projection_periods)

    for sheet_name, cells in workbook.items():
        rows = sheet_row_to_item.get(sheet_name, {})
        line_items: List[LineItem] = []
        label_col = sheet_label_col.get(sheet_name, 1)
        for row, line_item_id in rows.items():
            label = _get_cell_value(cells, row, label_col) or ""
            is_spacer = "._spacer_r" in line_item_id
            item = LineItem(
                id=line_item_id,
                label=label,
                row=row,
                item_type=ItemType.input,
                unit=Unit.dollars,
                format="",
            )

            # Spacer rows are blank rows referenced by formulas — seed with 0
            if is_spacer:
                item.item_type = ItemType.input
                item.values = ValueSeries(values={
                    period: ValueCell(period=period, value=0.0, provenance=ValueProvenance.imported_other)
                    for period in all_periods
                })
                line_items.append(item)
                continue

            formulas_by_period = formulas_by_row.get((sheet_name, row), {})
            values = values_by_row.get((sheet_name, row))

            if values and values.values:
                item.values = values

            if formulas_by_period:
                item.item_type = ItemType.derived
                item.formula_periods = sorted(formulas_by_period.keys())
                hist_spec, hist_overrides = _choose_formula(formulas_by_period, historical_periods)
                proj_spec, proj_overrides = _choose_formula(formulas_by_period, projection_periods)
                item.historical = hist_spec
                item.projected = proj_spec

                all_overrides: Dict[int, FormulaSpec] = {}
                all_overrides.update(hist_overrides)
                all_overrides.update(proj_overrides)

                if values and values.values:
                    for period in all_periods:
                        if period not in formulas_by_period and period in values.values:
                            cached = values.values[period]
                            if cached.value is not None:
                                all_overrides[period] = FormulaSpec(
                                    type=FormulaType.constant,
                                    params={"value": cached.value},
                                )
                item.overrides = all_overrides or None
            elif values and values.values:
                item.item_type = ItemType.input
            else:
                item.item_type = ItemType.header

            line_items.append(item)

        section = Section(id="main", label="Main", line_items=line_items)
        model.sheets[sheet_name] = Sheet(name=sheet_name, sections=[section])

    if mode == "full":
        return model

    return {
        "sheets": list(model.sheets.keys()),
        "line_item_count": sum(len(sheet.sections[0].line_items) for sheet in model.sheets.values()),
        "years": time_structure.historical_years + time_structure.projection_years,
        "time_keys": historical_periods + projection_periods,
        "period_mode": time_structure.period_mode,
        "formula_counts": classification_counts,
    }


# Helpers


def _load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    """Read shared strings table from xlsx (if present)."""
    try:
        xml_data = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(xml_data)
    strings: List[str] = []
    for si in root.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
        parts = []
        for t in si.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"):
            if t.text:
                parts.append(t.text)
        strings.append("".join(parts))
    return strings


def _load_sheets(zf: zipfile.ZipFile) -> Dict[str, str]:
    """Map sheet names to worksheet XML paths in the xlsx zip."""
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

    sheets: Dict[str, str] = {}
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    for sheet in workbook.findall(f".//{ns}sheet"):
        name = sheet.attrib["name"]
        rel_id = sheet.attrib.get(f"{rel_ns}id")
        target = rel_map.get(rel_id)
        if not target:
            continue
        sheet_path = f"xl/{target}"
        sheets[name] = sheet_path
    return sheets


def _parse_sheet(xml_data: bytes, shared_strings: List[str]) -> Dict[Tuple[int, int], CellData]:
    """Parse a worksheet XML into cell value/formula records.

    Note: Excel's shared formula optimization means slave cells (which reference
    a master via ``si`` index but carry no formula text) will have formula=None.
    These are intentionally left as-is — their cached values are used instead.
    Expanding shared formulas is possible via ``_translate_formula`` but is not
    done here because it removes constant-override safety nets that anchor
    dependency chains.
    """
    root = ET.fromstring(xml_data)
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    cells: Dict[Tuple[int, int], CellData] = {}

    for row in root.findall(f".//{ns}row"):
        for cell in row.findall(f"{ns}c"):
            cell_ref = cell.attrib.get("r")
            if not cell_ref:
                continue
            col_letters, row_num = _split_cell(cell_ref)
            col_idx = _col_to_index(col_letters)
            value_node = cell.find(f"{ns}v")
            formula_node = cell.find(f"{ns}f")
            value = None
            if value_node is not None and value_node.text is not None:
                value = value_node.text
                if cell.attrib.get("t") == "s":
                    try:
                        value = shared_strings[int(value)]
                    except (ValueError, IndexError):
                        pass
            if cell.attrib.get("t") == "inlineStr":
                inline = cell.find(f"{ns}is")
                if inline is not None:
                    texts = [t.text for t in inline.findall(f".//{ns}t") if t.text]
                    value = "".join(texts)

            formula = formula_node.text if formula_node is not None else None
            cells[(row_num, col_idx)] = CellData(value=value, formula=formula)

    return cells


def _translate_formula(formula: str, row_offset: int, col_offset: int) -> str:
    """Translate cell references in a formula by row/col offset.

    Handles absolute references ($A$1 stays fixed), mixed ($A1 shifts row only,
    A$1 shifts col only), and relative (A1 shifts both).
    """
    def _translate_match(m: re.Match) -> str:
        col_abs = m.group(1) or ""  # "$" or ""
        col_str = m.group(2)
        row_abs = m.group(3) or ""  # "$" or ""
        row_str = m.group(4)

        if col_abs != "$" and col_offset != 0:
            col_idx = _col_to_index(col_str) + col_offset
            if col_idx < 1:
                col_idx = 1
            col_str = _index_to_col(col_idx)

        if row_abs != "$" and row_offset != 0:
            new_row = int(row_str) + row_offset
            if new_row < 1:
                new_row = 1
            row_str = str(new_row)

        return f"{col_abs}{col_str}{row_abs}{row_str}"

    # Match cell references: optional $, col letters, optional $, row digits
    # Negative lookbehind for alphanumeric to avoid matching inside function names
    return re.sub(
        r"(?<![A-Za-z0-9_])(\$?)([A-Z]{1,3})(\$?)(\d+)(?![A-Za-z0-9_\(])",
        _translate_match,
        formula,
    )


def _map_referenced_blank_rows(
    sheet_name: str,
    cells: Dict[Tuple[int, int], CellData],
    sheet_row_to_item: Dict[str, Dict[int, str]],
) -> None:
    """Detect blank spacer rows referenced by formulas and map them as items.

    Some models have blank rows (no label) that are referenced by formulas like
    ``D5+D6`` where Row 6 is a spacer.  If these rows have no label and no
    non-zero values, we add them as zero-value spacer items so formula
    resolution doesn't fall back to ``raw``.
    """
    row_to_item = sheet_row_to_item.get(sheet_name, {})
    sheet_prefix = _slugify(sheet_name)

    # Collect all rows referenced by formulas in this sheet
    referenced_rows: set = set()
    cell_ref_pattern = re.compile(r"(?<![A-Za-z0-9_])(\$?)([A-Z]{1,3})(\$?)(\d+)(?![A-Za-z0-9_\(])")
    for (_row, _col), cell in cells.items():
        if not cell.formula:
            continue
        for m in cell_ref_pattern.finditer(cell.formula):
            # Only consider refs within same sheet (no sheet prefix before match)
            start = m.start()
            if start > 0 and cell.formula[start - 1] == "!":
                continue
            ref_row = int(m.group(4))
            referenced_rows.add(ref_row)

    # Find referenced rows that are not mapped and appear blank
    for ref_row in referenced_rows:
        if ref_row in row_to_item:
            continue

        # Check if this row has any non-empty label
        label_col = _detect_label_column(cells)
        label = _get_cell_value(cells, ref_row, label_col)
        if label and str(label).strip():
            continue  # Has a label — should have been picked up; don't force-add

        # Check if row has any non-zero numeric values
        has_nonzero = False
        for (_r, _c), cell in cells.items():
            if _r != ref_row or _c <= 3:  # skip label columns
                continue
            if cell.value is not None:
                try:
                    val = float(cell.value)
                    if val != 0.0:
                        has_nonzero = True
                        break
                except ValueError:
                    has_nonzero = True
                    break

        if not has_nonzero:
            # Map as a zero-value spacer item
            item_id = f"{sheet_prefix}._spacer_r{ref_row}"
            row_to_item[ref_row] = item_id


def _detect_label_column(cells: Dict[Tuple[int, int], CellData]) -> int:
    """Detect which column contains row labels (usually A=1 or B=2).

    Heuristic: find the leftmost column (among 1-3) with the most non-empty
    text values that don't look like numbers or years.
    """
    col_scores: Dict[int, int] = {}
    for (row, col_idx), cell in cells.items():
        if col_idx > 3 or row <= 2:
            continue
        if cell.value and str(cell.value).strip():
            text = str(cell.value).strip()
            # Skip if it looks numeric
            try:
                float(text)
                continue
            except ValueError:
                pass
            col_scores[col_idx] = col_scores.get(col_idx, 0) + 1
    if not col_scores:
        return 1
    return min(col_scores, key=lambda c: (-(col_scores[c]), c))


def _extract_line_items(sheet_name: str, cells: Dict[Tuple[int, int], CellData]) -> Tuple[Dict[int, str], List[int]]:
    """Extract line-item IDs from the label column (auto-detected)."""
    label_col = _detect_label_column(cells)
    row_to_item: Dict[int, str] = {}
    label_rows: List[int] = []
    sheet_prefix = _slugify(sheet_name)
    for (row, col_idx), cell in cells.items():
        if col_idx != label_col:
            continue
        label = cell.value
        if not label or str(label).strip() == "":
            continue
        label_rows.append(row)
        item_id = f"{sheet_prefix}.{_slugify(str(label))}"
        if item_id in row_to_item.values():
            item_id = f"{item_id}_r{row}"
        row_to_item[row] = item_id
    return row_to_item, label_rows


_QUARTERLY_RE = re.compile(r"^[1-4]Q\d{2,4}[AEae]?$")


_QUARTERLY_AUTO_THRESHOLD = 4  # Minimum quarterly tokens to trigger auto-detection


def _sheet_has_quarterly_tokens(cells: Dict[Tuple[int, int], CellData]) -> bool:
    """Detect quarterly columns in header rows.

    Requires at least _QUARTERLY_AUTO_THRESHOLD quarterly tokens in a single
    row to avoid false positives from stray '1Q24'-like labels in notes cells.
    """
    for row in range(1, 11):
        count = 0
        for col in range(2, 200):
            parsed = _parse_period_token(_get_cell_value(cells, row, col))
            if parsed and parsed[2]:
                count += 1
                if count >= _QUARTERLY_AUTO_THRESHOLD:
                    return True
    return False


def _find_period_header(
    cells: Dict[Tuple[int, int], CellData],
    mode: str,
) -> Tuple[Dict[int, int], Set[int], Set[int]]:
    """Heuristically locate period header rows and return period mappings."""
    rows_data = []
    for row in range(1, 11):
        periods: Dict[int, int] = {}
        quarterly_cols: Set[int] = set()
        annual_cols: Set[int] = set()
        for col in range(2, 200):
            value = _get_cell_value(cells, row, col)
            parsed = _parse_period_token(value)
            if parsed:
                year, slot, is_quarterly = parsed
                period = encode_period(year, slot or 5, mode)
                periods[col] = period
                if is_quarterly:
                    quarterly_cols.add(col)
                else:
                    annual_cols.add(col)
        rows_data.append((row, periods, quarterly_cols, annual_cols))

    rows_data.sort(key=lambda x: -len(x[1]))
    if not rows_data or not rows_data[0][1]:
        return {}, set(), set()

    primary_periods = rows_data[0][1]
    primary_quarterly = rows_data[0][2]
    primary_annual = rows_data[0][3]

    if not primary_quarterly:
        return primary_periods, set(), set(primary_annual)

    merged = dict(primary_periods)
    all_quarterly = set(primary_quarterly)
    all_annual = set(primary_annual)
    primary_count = len(primary_periods)
    for _, periods, qcols, acols in rows_data[1:3]:
        if len(periods) >= max(3, primary_count // 4):
            for col, period in periods.items():
                if col not in merged:
                    merged[col] = period
            all_quarterly.update(qcols)
            all_annual.update(acols)

    return merged, all_quarterly, all_annual


def _find_year_header(cells: Dict[Tuple[int, int], CellData]) -> Tuple[Dict[int, int], Set[int]]:
    """Backward-compatible wrapper for legacy tests."""
    col_to_period, quarterly_cols, _ = _find_period_header(cells, PERIOD_MODE_YEARLY)
    return col_to_period, quarterly_cols


def _build_time_structure(
    periods: List[int],
    period_mode: str = PERIOD_MODE_YEARLY,
    historical_cutoff_year: Optional[int] = None,
) -> TimeStructure:
    """Build a TimeStructure from detected period keys."""
    if not periods:
        return TimeStructure(
            fiscal_year_end="",
            period_mode=period_mode,
            historical_periods=[],
            projection_periods=[],
            period_column_map={},
            historical_years=[],
            projection_years=[],
            column_map={},
        )

    periods_sorted = sorted(set(periods))
    historical_periods: List[int] = []
    projection_periods: List[int] = []
    if historical_cutoff_year is not None:
        historical_periods = [
            period for period in periods_sorted if period_year(period, period_mode) <= historical_cutoff_year
        ]
        projection_periods = [
            period for period in periods_sorted if period_year(period, period_mode) > historical_cutoff_year
        ]
    else:
        historical_periods = periods_sorted

    if period_mode == PERIOD_MODE_YEARLY:
        historical_years = list(historical_periods)
        projection_years = list(projection_periods)
    else:
        historical_years = sorted({period_year(period, period_mode) for period in historical_periods})
        projection_years = sorted({period_year(period, period_mode) for period in projection_periods})

    all_years = sorted(set(historical_years + projection_years))
    column_map = {year: _index_to_col(idx + 1) for idx, year in enumerate(all_years)}
    period_column_map = {period: _index_to_col(idx + 1) for idx, period in enumerate(periods_sorted)}
    return TimeStructure(
        fiscal_year_end="",
        period_mode=period_mode,
        historical_periods=historical_periods,
        projection_periods=projection_periods,
        period_column_map=period_column_map,
        historical_years=historical_years,
        projection_years=projection_years,
        column_map=column_map,
    )


def _choose_formula(
    formulas_by_year: Dict[int, FormulaSpec],
    years: List[int],
) -> Tuple[Optional[FormulaSpec], Dict[int, FormulaSpec]]:
    """Pick a representative FormulaSpec for the given year set."""
    if not formulas_by_year:
        return None, {}

    candidate_years = [year for year in years if year in formulas_by_year] if years else list(formulas_by_year.keys())
    if not candidate_years:
        return None, {}

    signature_counts: Dict[Tuple[FormulaType, Optional[str], object], List[int]] = {}
    for year in candidate_years:
        spec = formulas_by_year[year]
        signature = (spec.type, spec.subtype, _param_shape(spec.params))
        signature_counts.setdefault(signature, []).append(year)

    best_signature = None
    best_count = -1
    best_latest_year = -1
    for signature, sig_years in signature_counts.items():
        count = len(sig_years)
        latest_year = max(sig_years)
        if count > best_count or (count == best_count and latest_year > best_latest_year):
            best_signature = signature
            best_count = count
            best_latest_year = latest_year

    if best_signature is None:
        return None, {}

    representative_year = max(signature_counts[best_signature])
    representative_spec = formulas_by_year[representative_year]
    overrides = {}
    for year in candidate_years:
        spec = formulas_by_year[year]
        signature = (spec.type, spec.subtype, _param_shape(spec.params))
        if signature != best_signature:
            overrides[year] = spec

    return representative_spec, overrides


def _param_shape(value):
    """Normalize formula params into a hashable shape."""
    if isinstance(value, LineItemRef):
        return ("ref", value.id, value.t, value.resolved)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("num", round(float(value), 6))
    if value is None:
        return ("none",)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, dict):
        if "op" in value and isinstance(value.get("args"), list):
            op = value.get("op")
            args = list(value.get("args", []))
            if op in {"+", "*", "SUM", "AVG"}:
                args = sorted(args, key=lambda arg: _shape_sort_key(_param_shape(arg)))
            normalized = dict(value)
            normalized["args"] = args
            return ("dict", tuple((key, _param_shape(normalized[key])) for key in sorted(normalized)))
        if value.get("function") in {"SUM", "AVERAGE"} and isinstance(value.get("items"), list):
            items = list(value.get("items", []))
            items = sorted(items, key=lambda item: _shape_sort_key(_param_shape(item)))
            normalized = dict(value)
            normalized["items"] = items
            return ("dict", tuple((key, _param_shape(normalized[key])) for key in sorted(normalized)))
        return ("dict", tuple((key, _param_shape(value[key])) for key in sorted(value)))
    if isinstance(value, set):
        items = sorted(value, key=lambda item: _shape_sort_key(_param_shape(item)))
        return ("list", tuple(_param_shape(item) for item in items))
    if isinstance(value, (list, tuple)):
        return ("list", tuple(_param_shape(item) for item in value))
    return ("type", type(value).__name__)


def _shape_sort_key(value) -> str:
    return repr(value)


def _collect_refs(value, refs: List[LineItemRef]) -> None:
    if isinstance(value, LineItemRef):
        refs.append(value)
        return
    if isinstance(value, dict):
        for v in value.values():
            _collect_refs(v, refs)
        return
    if isinstance(value, (list, tuple, set)):
        for v in value:
            _collect_refs(v, refs)


def _is_self_referencing(spec: Optional[FormulaSpec], line_item_id: str) -> bool:
    """Check if a formula references its own line item at t=0."""
    if not spec:
        return False
    refs: List[LineItemRef] = []
    _collect_refs(spec.params, refs)
    if not refs:
        return False
    return any(ref.id == line_item_id and ref.t == 0 for ref in refs)


def _dedup_additive_refs(spec: FormulaSpec) -> FormulaSpec:
    if spec.type != FormulaType.arithmetic:
        return spec

    params = spec.params if isinstance(spec.params, dict) else {}
    changed = False

    def _dedup_expr(expr):
        if not isinstance(expr, dict):
            return expr

        op = expr.get("op")
        if op is None:
            return expr

        if op in {"+", "SUM", "AVG", "AVERAGE"}:
            args = expr.get("args", [])
            if not isinstance(args, list):
                return expr
            new_args = []
            for arg in args:
                new_args.append(_dedup_expr(arg))

            seen: Set[Tuple[str, int]] = set()
            deduped = []
            for arg in new_args:
                if isinstance(arg, LineItemRef):
                    key = (arg.id, arg.t)
                    if key in seen:
                        continue
                    seen.add(key)
                deduped.append(arg)

            if deduped != args or any(new is not old for new, old in zip(new_args, args)):
                return {"op": op, "args": deduped}
            return expr

        if op in {"-", "/", "^"}:
            left = expr.get("left")
            right = expr.get("right")
            new_left = _dedup_expr(left)
            new_right = _dedup_expr(right)
            if new_left is not left or new_right is not right:
                return {"op": op, "left": new_left, "right": new_right}
            return expr

        if op == "NEG":
            arg = expr.get("arg")
            new_arg = _dedup_expr(arg)
            if new_arg is not arg:
                return {"op": "NEG", "arg": new_arg}
            return expr

        if op == "*":
            args = expr.get("args", [])
            if not isinstance(args, list):
                return expr
            new_args = [_dedup_expr(arg) for arg in args]
            if any(new is not old for new, old in zip(new_args, args)):
                return {"op": "*", "args": new_args}
            return expr

        return expr

    def _dedup_items(items: object) -> object:
        nonlocal changed
        if not isinstance(items, list):
            return items

        seen: Set[Tuple[str, int]] = set()
        deduped = []
        for item in items:
            new_item = _dedup_expr(item) if isinstance(item, dict) else item
            if isinstance(new_item, LineItemRef):
                key = (new_item.id, new_item.t)
                if key in seen:
                    changed = True
                    continue
                seen.add(key)
            if new_item is not item:
                changed = True
            deduped.append(new_item)
        if len(deduped) != len(items):
            changed = True
        return deduped

    new_params = params

    function = params.get("function")
    if isinstance(function, str) and function.upper() in {"SUM", "AVERAGE", "AVG"} and "items" in params:
        new_items = _dedup_items(params.get("items"))
        if new_items is not params.get("items"):
            new_params = dict(params)
            new_params["items"] = new_items
    elif "items" in params and "function" not in params:
        new_items = _dedup_items(params.get("items"))
        if new_items is not params.get("items"):
            new_params = dict(params)
            new_params["items"] = new_items
    elif "operands" in params:
        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            operator = operands[0]
            if operator == "+":
                seen: Set[Tuple[str, int]] = set()
                new_operands = [operator]
                for operand in operands[1:]:
                    if isinstance(operand, LineItemRef):
                        key = (operand.id, operand.t)
                        if key in seen:
                            changed = True
                            continue
                        seen.add(key)
                    new_operands.append(operand)
                if len(new_operands) != len(operands):
                    changed = True
                if changed:
                    new_params = dict(params)
                    new_params["operands"] = new_operands
    elif "expr" in params:
        expr = params.get("expr")
        new_expr = _dedup_expr(expr)
        if new_expr is not expr:
            changed = True
            new_params = dict(params)
            new_params["expr"] = new_expr

    if not changed:
        return spec
    return FormulaSpec(
        type=spec.type,
        subtype=spec.subtype,
        params=new_params,
        note=spec.note,
    )


def _get_cell_value(cells: Dict[Tuple[int, int], CellData], row: int, col: int) -> Optional[str]:
    """Return the raw cell value if present."""
    cell = cells.get((row, col))
    if cell is None:
        return None
    return cell.value


def _parse_year(value: Optional[str]) -> Optional[int]:
    """Backward-compatible wrapper for legacy tests."""
    parsed = _parse_period_token(value)
    if not parsed:
        return None
    return parsed[0]


def _normalize_year_token(token: str) -> Optional[int]:
    if not token:
        return None
    if len(token) == 2:
        year_suffix = int(token)
        year = 2000 + year_suffix if year_suffix < 80 else 1900 + year_suffix
    elif len(token) == 3:
        trimmed = token.lstrip("0") or "0"
        if len(trimmed) <= 2:
            year_suffix = int(trimmed)
            year = 2000 + year_suffix if year_suffix < 80 else 1900 + year_suffix
        else:
            year = int(trimmed)
    else:
        year = int(token)
    if 1900 <= year <= 2100:
        return year
    return None


def _parse_period_token(value: Optional[str]) -> Optional[Tuple[int, Optional[int], bool]]:
    """Parse a header token into (year, slot-or-none, is_quarterly-token)."""
    if value is None:
        return None
    text = str(value).strip()
    m = re.match(r"^([1-4])Q(\d{2,4})[AEae]?$", text)
    if m:
        slot = int(m.group(1))
        year_str = m.group(2)
        year = _normalize_year_token(year_str)
        if year is not None:
            return year, slot, True
    try:
        if text and text[-1] in "AEae":
            text = text[:-1]
        num = int(float(text))
        # For non-quarterly tokens, only accept explicit 4-digit years.
        # 2-digit normalization (e.g. 90→1990, 57→2057) is too aggressive
        # and misidentifies numeric data as years.
        if 1900 <= num <= 2100:
            return num, None, False
    except ValueError:
        return None
    return None


def _coerce_number(value: Optional[str]) -> Optional[float]:
    """Convert a string to float when possible."""
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _split_cell(cell_ref: str) -> Tuple[str, int]:
    col = ""
    row = ""
    for ch in cell_ref:
        if ch.isalpha():
            col += ch
        else:
            row += ch
    return col, int(row)


def _col_to_index(col: str) -> int:
    col = col.upper()
    index = 0
    for ch in col:
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index


def _index_to_col(idx: int) -> str:
    letters = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters.append(chr(rem + ord("A")))
    return "".join(reversed(letters))


def _slugify(text: str) -> str:
    text = text.strip().lower()
    out = []
    prev_underscore = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out.append("_")
                prev_underscore = True
    slug = "".join(out).strip("_")
    return slug or "line_item"
