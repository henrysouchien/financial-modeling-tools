"""Serialize renderer plans into deterministic XLSX bytes or files.

Ingress is ``schema.renderer.RenderPlan``. ``render_xlsx_bytes`` has no
filesystem effect; ``write_xlsx`` is the compatibility file adapter. Neither
function authorizes or records an FMS product-state mutation.

Owner and build-pipeline guide: ``schema/README.md``.
"""

from __future__ import annotations

from copy import copy
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import re
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from openpyxl import Workbook
from openpyxl.packaging.custom import CustomPropertyList, StringProperty
from openpyxl.styles import Border, Font, PatternFill, Side
from openpyxl.utils import range_boundaries

from .model_ref import ModelStamp
from .renderer import CellFormat, RenderPlan


_CANONICAL_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def render_xlsx_bytes(
    plan: RenderPlan,
    *,
    finalized_at: datetime,
    stamp: ModelStamp | None = None,
) -> bytes:
    """Return deterministic XLSX bytes without touching the filesystem.

    ``finalized_at`` must be timezone-aware because it participates in the
    canonical archive metadata; malformed plans or archives raise rather than
    producing best-effort bytes.

    ``stamp`` is the published ModelVersion identity. It is written as the
    custom document properties ``hank_model_id``, ``hank_model_version`` and
    ``hank_build_id`` — plain strings with no timestamp, so the bytes stay
    deterministic and the publish digest covers them. A render with no
    version identity (a bench or compatibility render) passes ``None`` and
    carries no stamp, which is how a reader tells a hand-built workbook.
    """

    if finalized_at.tzinfo is None or finalized_at.utcoffset() is None:
        raise ValueError("finalized_at must be timezone-aware")
    canonical_time = finalized_at.astimezone(timezone.utc).replace(tzinfo=None)

    workbook = Workbook()
    workbook.properties.created = canonical_time
    workbook.properties.modified = canonical_time
    workbook.properties.creator = "ai-excel-addin"
    workbook.properties.lastModifiedBy = "ai-excel-addin"
    if stamp is not None:
        # openpyxl ships no py.typed, so pyright scores any direct
        # ``workbook.custom_doc_props`` as an unknown attribute. The value is a
        # typed ``CustomPropertyList``; the read/write goes through the untyped
        # third-party attribute only, which ``setattr``/``getattr`` accept.
        custom_props = CustomPropertyList()
        for name, value in (
            ("hank_model_id", stamp.model_id),
            ("hank_model_version", str(stamp.version)),
            ("hank_build_id", stamp.build_id),
        ):
            custom_props.append(StringProperty(name=name, value=value))
        setattr(workbook, "custom_doc_props", custom_props)
    default_sheet = workbook["Sheet"]
    default_sheet.title = "Sheet"

    for setup in plan.sheet_setups:
        worksheet = workbook.create_sheet(title=setup.sheet)
        for column, width in setup.column_widths.items():
            worksheet.column_dimensions[column].width = width
        if setup.freeze_panes:
            worksheet.freeze_panes = setup.freeze_panes

    for write in plan.writes:
        worksheet = _worksheet_for(workbook, write.sheet)
        worksheet[write.cell].value = write.value

    for cell_format in plan.formats:
        worksheet = _worksheet_for(workbook, cell_format.sheet)
        _apply_format(worksheet, cell_format)

    if "Sheet" in workbook.sheetnames and len(workbook.sheetnames) > 1:
        workbook.remove(workbook["Sheet"])

    raw = BytesIO()
    workbook.save(raw)
    workbook.close()
    return _canonicalize_xlsx_archive(
        raw.getvalue(),
        finalized_at=finalized_at,
    )


def write_xlsx(
    plan: RenderPlan,
    output_path: str,
    *,
    finalized_at: datetime | None = None,
) -> None:
    """Persist deterministic XLSX bytes at the caller-selected path.

    This is a filesystem compatibility adapter, not the authenticated durable
    mutation boundary; FMS owns product commits and lineage.
    """

    canonical_time = finalized_at or datetime(1980, 1, 1, tzinfo=timezone.utc)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_bytes(
        render_xlsx_bytes(plan, finalized_at=canonical_time)
    )


def _canonicalize_xlsx_archive(
    payload: bytes,
    *,
    finalized_at: datetime,
) -> bytes:
    canonical_core_timestamp = (
        finalized_at.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
        .encode("ascii")
    )
    source = BytesIO(payload)
    target = BytesIO()
    with ZipFile(source, "r") as incoming, ZipFile(
        target,
        "w",
        compression=ZIP_DEFLATED,
        compresslevel=9,
        strict_timestamps=True,
    ) as outgoing:
        for name in sorted(incoming.namelist()):
            content = incoming.read(name)
            if name == "docProps/core.xml":
                for property_name in (b"created", b"modified"):
                    pattern = re.compile(
                        rb"(<dcterms:"
                        + property_name
                        + rb"\b[^>]*>)[^<]*(</dcterms:"
                        + property_name
                        + rb">)"
                    )
                    content, replacement_count = pattern.subn(
                        lambda match: (
                            match.group(1)
                            + canonical_core_timestamp
                            + match.group(2)
                        ),
                        content,
                    )
                    if replacement_count != 1:
                        raise ValueError(
                            "XLSX core properties must contain exactly one "
                            f"{property_name.decode('ascii')} timestamp"
                        )
            info = ZipInfo(filename=name, date_time=_CANONICAL_ZIP_TIMESTAMP)
            info.compress_type = ZIP_DEFLATED
            info.create_system = 0
            info.create_version = 20
            info.extract_version = 20
            info.external_attr = 0
            info.internal_attr = 0
            info.extra = b""
            info.comment = b""
            outgoing.writestr(
                info,
                content,
                compress_type=ZIP_DEFLATED,
                compresslevel=9,
            )
    return target.getvalue()


def _worksheet_for(workbook: Workbook, sheet_name: str):
    if sheet_name in workbook.sheetnames:
        return workbook[sheet_name]
    return workbook.create_sheet(title=sheet_name)


def _apply_format(worksheet, cell_format: CellFormat) -> None:
    min_col, min_row, max_col, max_row = range_boundaries(cell_format.range)
    font_color = _normalize_font_color(cell_format.font_color)

    for row in worksheet.iter_rows(
        min_row=min_row,
        max_row=max_row,
        min_col=min_col,
        max_col=max_col,
    ):
        for cell in row:
            if cell_format.number_format is not None:
                cell.number_format = cell_format.number_format
            fill_color = _normalize_color(cell_format.fill_color, field="fill_color")
            if fill_color is not None:
                cell.fill = PatternFill(fill_type="solid", fgColor=fill_color)

            top_border_color = _normalize_color(
                cell_format.top_border_color,
                field="top_border_color",
            )
            bottom_border_color = _normalize_color(
                cell_format.bottom_border_color,
                field="bottom_border_color",
            )
            if top_border_color is not None or bottom_border_color is not None:
                border = copy(cell.border) if cell.border is not None else Border()
                if top_border_color is not None:
                    border.top = Side(style="thin", color=top_border_color)
                if bottom_border_color is not None:
                    border.bottom = Side(style="thin", color=bottom_border_color)
                cell.border = border

            if cell_format.bold is None and font_color is None and cell_format.underline is None:
                continue

            font = copy(cell.font) if cell.font is not None else Font()
            if cell_format.bold is not None:
                font.bold = cell_format.bold
            if font_color is not None:
                font.color = font_color
            if cell_format.underline is not None:
                font.underline = "single" if cell_format.underline else None
            cell.font = font


def _normalize_font_color(color: str | None) -> str | None:
    return _normalize_color(color, field="font_color")


def _normalize_color(color: str | None, *, field: str) -> str | None:
    if not color:
        return None
    normalized = color.lstrip("#").upper()
    if len(normalized) == 6:
        return normalized
    if len(normalized) == 8:
        return normalized[-6:]
    raise ValueError(f"Unsupported {field}: {color}")


__all__ = ["render_xlsx_bytes", "write_xlsx"]
