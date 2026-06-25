"""Excel → Schema reader utilities.

Purpose:
- Parse .xlsx files and build a minimal FinancialModel.
- Extract labels, formulas, values, and time structure from sheet XML.
- Classify formulas using FormulaPatternMatcher and attach FormulaSpecs.

The reader is intentionally heuristic:
- Year headers are inferred from the densest row of year-like values.
- Column A labels are slugified into line_item_ids.
- Shared formulas can be expanded in opt-in mode; default mode uses cached values.
- Named ranges and structured references are not resolved.

Example:
Row label: "Gross Profit" (row 9) → line_item_id: "gross_profit"
Cell H9 formula: =Assumptions!H34 → FormulaSpec(type=ref, source=LineItemRef("gross_profit", t=0))
"""

from __future__ import annotations

import hashlib
import posixpath
import pathlib
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
    LineItemRef,  # noqa: F401 - compatibility alias for schema.reader imports
    ModelMetadata,
    ScenarioInputs,  # noqa: F401 - compatibility alias for schema.reader imports
    Section,
    Sheet,
    TimeStructure,  # noqa: F401 - compatibility alias for schema.reader imports
    Unit,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    PERIOD_MODE_QUARTERLY5,
    PERIOD_MODE_YEARLY,
    encode_period,  # noqa: F401 - compatibility alias for schema.reader imports
    period_year,  # noqa: F401 - compatibility alias for schema.reader imports
)
from .pattern_matcher import CellContext, FormulaPatternMatcher
from .reader_cells import (
    _coerce_number,
    _col_to_index,
    _get_cell_value,
    _index_to_col,
    _normalize_year_token,  # noqa: F401 - compatibility alias for schema.reader imports
    _parse_period_token,  # noqa: F401 - compatibility alias for schema.reader imports
    _slugify,
    _split_cell,
)
from .reader_formula import (
    _choose_formula,
    _collect_refs,  # noqa: F401 - compatibility alias for schema.reader imports
    _dedup_additive_refs,
    _is_expanded_shared_raw,
    _is_self_referencing,
    _param_shape,
    _shape_sort_key,  # noqa: F401 - compatibility alias for schema.reader imports
)
from .reader_periods import (
    _QUARTERLY_AUTO_THRESHOLD as _QUARTERLY_AUTO_THRESHOLD,
    _QUARTERLY_RE as _QUARTERLY_RE,
    _build_time_structure as _build_time_structure,
    _expand_date_header_periods as _expand_date_header_periods,
    _find_period_header as _find_period_header,
    _is_weak_annual_period_header as _is_weak_annual_period_header,
    _parse_excel_date_period_token as _parse_excel_date_period_token,
    _row_is_date_period_header as _row_is_date_period_header,
    _sheet_has_quarterly_tokens as _sheet_has_quarterly_tokens,
    _sheet_uses_period_columns as _sheet_uses_period_columns,
)


_PARSER_MANIFEST: list[str] = [
    "reader.py",
    "reader_cells.py",
    "reader_formula.py",
    "reader_periods.py",
    "pattern_matcher.py",
    "formula_ast.py",
    "models.py",
]
_PARSER_DIR = pathlib.Path(__file__).resolve().parent


def _compute_reader_version() -> str:
    h = hashlib.sha1()
    for name in _PARSER_MANIFEST:
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update((_PARSER_DIR / name).read_bytes())
        h.update(b"\0")
    return h.hexdigest()[:16]


READER_VERSION: str = _compute_reader_version()


@dataclass
class CellData:
    value: Optional[str]
    formula: Optional[str]
    expanded_shared: bool = False


@dataclass
class SharedFormulaMaster:
    formula: str
    row: int
    col: int
    ref_range: Optional[str]


@dataclass
class PendingSlave:
    row: int
    col: int
    si: str


class ExcelWorkbookReader:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def read(self, expand_shared: bool = False) -> Dict[str, Dict[Tuple[int, int], CellData]]:
        """Load an .xlsx into a dict of sheet -> {(row, col): CellData}."""
        with zipfile.ZipFile(self.file_path) as zf:
            shared_strings = _load_shared_strings(zf)
            sheets = _load_sheets(zf)

            data: Dict[str, Dict[Tuple[int, int], CellData]] = {}
            for sheet_name, sheet_path in sheets.items():
                xml_data = zf.read(sheet_path)
                data[sheet_name] = _parse_sheet(
                    xml_data,
                    shared_strings,
                    expand_shared=expand_shared,
                )
        return data


def read_model(
    file_path: str,
    mode: str = "quick",
    historical_cutoff_year: Optional[int] = None,
    quarterly_mode: str = "auto",
    expand_shared: bool = False,
) -> Dict:
    """Parse an Excel model into a schema (quick summary or full model)."""
    reader = ExcelWorkbookReader(file_path)
    workbook = reader.read() if not expand_shared else reader.read(expand_shared=True)
    matcher = FormulaPatternMatcher()

    if quarterly_mode not in {"yearly", "auto", "quarterly_native"}:
        raise ValueError("quarterly_mode must be one of: yearly, auto, quarterly_native")

    workbook_has_quarterly = any(_sheet_has_quarterly_tokens(cells) for cells in workbook.values())
    if quarterly_mode == "yearly":
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

    fallback_col_to_period = max(sheet_col_to_period.values(), key=len, default={})
    fallback_annual_cols = {
        col
        for sheet_name, col_to_period in sheet_col_to_period.items()
        if col_to_period == fallback_col_to_period
        for col in sheet_annual_cols.get(sheet_name, set())
    }
    if fallback_col_to_period:
        for sheet_name, cells in workbook.items():
            existing_col_to_period = sheet_col_to_period.get(sheet_name, {})
            existing_is_weak = bool(existing_col_to_period) and _is_weak_annual_period_header(
                cells,
                existing_col_to_period,
                sheet_quarterly_cols.get(sheet_name, set()),
            )
            if existing_col_to_period and not existing_is_weak:
                continue
            if existing_is_weak and len(fallback_col_to_period) < 3:
                continue
            if "financial" not in _slugify(sheet_name):
                continue
            if _sheet_uses_period_columns(cells, fallback_col_to_period):
                sheet_col_to_period[sheet_name] = dict(fallback_col_to_period)
                sheet_annual_cols[sheet_name] = set(fallback_annual_cols or fallback_col_to_period)
                sheet_quarterly_cols[sheet_name] = set()

    period_set = {
        period
        for col_to_period in sheet_col_to_period.values()
        for period in col_to_period.values()
    }

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
                    if cell.expanded_shared and spec.type == FormulaType.raw:
                        cached_value = _coerce_number(cell.value)
                        if cached_value is not None:
                            spec = FormulaSpec(
                                type=FormulaType.constant,
                                params={"value": cached_value},
                                note="expanded_shared_raw",
                            )
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
                vote_specs = [spec for spec in specs if not _is_expanded_shared_raw(spec)] or specs
                sig_counts: Dict[Tuple[FormulaType, Optional[str], object], int] = {}
                sig_to_spec: Dict[Tuple[FormulaType, Optional[str], object], FormulaSpec] = {}
                for spec in vote_specs:
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
        sheet_path = _resolve_workbook_relationship_target(target)
        sheets[name] = sheet_path
    return sheets


def _resolve_workbook_relationship_target(target: str) -> str:
    """Resolve a workbook relationship target to an xlsx archive member path."""
    target = str(target).strip()
    if target.startswith("/"):
        return posixpath.normpath(target.lstrip("/"))
    if target.startswith("xl/"):
        return posixpath.normpath(target)
    return posixpath.normpath(posixpath.join("xl", target))


def _parse_sheet(
    xml_data: bytes,
    shared_strings: List[str],
    expand_shared: bool = False,
) -> Dict[Tuple[int, int], CellData]:
    """Parse a worksheet XML into cell value/formula records.

    Note: Excel's shared formula optimization means slave cells (which reference
    a master via ``si`` index but carry no formula text) will have formula=None
    unless ``expand_shared=True``. Default mode preserves cached-value behavior.
    """
    root = ET.fromstring(xml_data)
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    cells: Dict[Tuple[int, int], CellData] = {}
    shared_masters: Dict[str, SharedFormulaMaster] = {}
    pending_slaves: List[PendingSlave] = []

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

            formula_text = formula_node.text if formula_node is not None else None
            formula_type = formula_node.get("t") if formula_node is not None else None
            si = formula_node.get("si") if formula_node is not None else None
            if formula_type == "shared" and formula_text and si is not None:
                shared_masters[si] = SharedFormulaMaster(
                    formula=formula_text,
                    row=row_num,
                    col=col_idx,
                    ref_range=formula_node.get("ref"),
                )
                formula = formula_text
            elif formula_type == "shared" and si is not None and not formula_text:
                pending_slaves.append(PendingSlave(row=row_num, col=col_idx, si=si))
                formula = None
            else:
                formula = formula_text
            cells[(row_num, col_idx)] = CellData(value=value, formula=formula)

    if expand_shared:
        for slave in pending_slaves:
            master = shared_masters.get(slave.si)
            if master is None:
                continue
            if master.ref_range and not _cell_in_range(slave.row, slave.col, master.ref_range):
                continue
            cell = cells.get((slave.row, slave.col))
            if cell is None:
                continue
            expanded = _translate_formula(
                master.formula,
                slave.row - master.row,
                slave.col - master.col,
            )
            cells[(slave.row, slave.col)] = CellData(
                value=cell.value,
                formula=expanded,
                expanded_shared=True,
            )

    return cells


def _cell_in_range(row: int, col: int, ref_range: str) -> bool:
    """Return whether the cell lies inside an A1 ref range like H7:S7."""
    try:
        if ":" in ref_range:
            start_ref, end_ref = ref_range.split(":", 1)
        else:
            start_ref = ref_range
            end_ref = ref_range
        start_col, start_row = _split_cell(start_ref.replace("$", ""))
        end_col, end_row = _split_cell(end_ref.replace("$", ""))
        start_col_idx = _col_to_index(start_col)
        end_col_idx = _col_to_index(end_col)
    except (AttributeError, TypeError, ValueError):
        return False
    return (
        min(start_row, end_row) <= row <= max(start_row, end_row)
        and min(start_col_idx, end_col_idx) <= col <= max(start_col_idx, end_col_idx)
    )


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

    zones: Dict[str, str] = {}
    zone_pattern = re.compile(r"""("(?:[^"]|"")*"|'(?:[^']|'')*'!|[A-Za-z_][\w.]*!)""")

    def _replace_zone(match: re.Match) -> str:
        zone = match.group(0)
        placeholder = f"@@zone{len(zones)}@@"
        suffix = 0
        while placeholder in formula or placeholder in zones:
            suffix += 1
            placeholder = f"@@zone{len(zones)}_{suffix}@@"
        zones[placeholder] = zone
        return placeholder

    sanitized = zone_pattern.sub(_replace_zone, formula)

    # Match cell references: optional $, col letters, optional $, row digits
    # Negative lookbehind for alphanumeric to avoid matching inside function names
    translated = re.sub(
        r"(?<![A-Za-z0-9_])(\$?)([A-Z]{1,3})(\$?)(\d+)(?![A-Za-z0-9_\(])",
        _translate_match,
        sanitized,
    )
    for placeholder, zone in zones.items():
        translated = translated.replace(placeholder, zone)
    return translated


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
