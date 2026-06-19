"""Pure renderer for industry peer comparison comp-sheet payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from statistics import median
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .renderer import CellFormat, CellWrite, RenderPlan, SheetSetup


FOCAL_FILL_COLOR = "#D9EAF7"

_NUMBER_FORMAT_BY_UNITS = {
    "usd_millions": "#,##0",
    "usd": "$#,##0",
    "percent": "0.0%",
    "multiple": "0.0x",
    "usd_per_share": "$0.00",
    "count": "#,##0",
}


class _PayloadModel(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)


class _PayloadCell(_PayloadModel):
    value: Any | None = None
    source_refs: list[str] = Field(default_factory=list)
    derived: bool = False


class _SnapshotMetric(_PayloadModel):
    key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    units: str | None = None
    values: dict[str, _PayloadCell] = Field(default_factory=dict)
    median: _PayloadCell | None = None


class _SnapshotSection(_PayloadModel):
    name: str = Field(min_length=1)
    metrics: list[_SnapshotMetric] = Field(default_factory=list)


class _TimeseriesMetric(_PayloadModel):
    key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    units: str | None = None
    series: dict[str, dict[int, _PayloadCell]] = Field(default_factory=dict)
    median_series: dict[int, _PayloadCell] = Field(default_factory=dict)


class _TimeseriesGroup(_PayloadModel):
    name: str = Field(min_length=1)
    metrics: list[_TimeseriesMetric] = Field(default_factory=list)


class _OperatingComparison(_PayloadModel):
    industry_key: str | None = None
    template_manifest_id: str = Field(min_length=1)
    years: list[int] = Field(default_factory=list)
    metric_groups: list[_TimeseriesGroup] = Field(default_factory=list)


class CompsPayload(_PayloadModel):
    """Payload emitted by risk_module.mcp_tools.industry._build_manifest_payload."""

    peers: list[dict[str, Any]] = Field(default_factory=list)
    industry_key: str | None = None
    template_manifest_id: str = Field(min_length=1)
    as_of: str | None = None
    sections: list[_SnapshotSection]
    sources: list[dict[str, Any]] = Field(default_factory=list)
    operating_comparison: _OperatingComparison | None = None

    @model_validator(mode="before")
    @classmethod
    def _reject_sections_less_payloads(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            raise ValueError("comps_payload must be a dict with a non-empty sections list")
        sections = value.get("sections")
        if not isinstance(sections, list) or not sections:
            raise ValueError(
                "comps_payload must include a non-empty 'sections' list from the "
                "v1.2 industry_peer_comparison payload. Legacy/gate-off payloads "
                "only include 'peers'; enable INDUSTRY_ANALYSIS_V1_2_ENABLED=true "
                "before calling industry_peer_comparison."
            )
        return value


@dataclass(frozen=True)
class CompsMetricCell:
    value: Any | None = None
    source_refs: tuple[str, ...] = field(default_factory=tuple)
    derived: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "source_refs": list(self.source_refs),
            "derived": self.derived,
        }


@dataclass(frozen=True)
class CompsMetricResult:
    key: str
    label: str
    units: str | None = None
    values: Mapping[str, CompsMetricCell] = field(default_factory=dict)
    median: CompsMetricCell | None = None
    series: Mapping[str, Mapping[int, CompsMetricCell]] = field(default_factory=dict)
    median_series: Mapping[int, CompsMetricCell] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "label": self.label,
            "units": self.units,
        }
        if self.values:
            payload["values"] = {
                ticker: cell.to_payload() for ticker, cell in self.values.items()
            }
            payload["median"] = self.median.to_payload() if self.median else None
        if self.series:
            payload["series"] = {
                ticker: {
                    str(year): cell.to_payload()
                    for year, cell in sorted(year_values.items())
                }
                for ticker, year_values in self.series.items()
            }
            payload["median_series"] = {
                str(year): cell.to_payload()
                for year, cell in sorted(self.median_series.items())
            }
        return payload


@dataclass(frozen=True)
class CompsGrid:
    template_id: str
    industry_key: str | None
    as_of: str | None
    tickers: tuple[str, ...]
    focal_ticker: str
    sections: tuple[tuple[str, tuple[CompsMetricResult, ...]], ...]
    operating_template_id: str | None = None
    years: tuple[int, ...] | None = None
    operating_sections: tuple[tuple[str, tuple[CompsMetricResult, ...]], ...] = field(
        default_factory=tuple
    )
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_operating(self) -> bool:
        return bool(self.operating_sections)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "template_id": self.template_id,
            "industry_key": self.industry_key,
            "as_of": self.as_of,
            "tickers": list(self.tickers),
            "focal_ticker": self.focal_ticker,
            "years": list(self.years) if self.years is not None else None,
            "sections": [
                {
                    "name": section_name,
                    "metrics": [metric.to_payload() for metric in metrics],
                }
                for section_name, metrics in self.sections
            ],
            "warnings": list(self.warnings),
        }
        if self.operating_sections:
            payload["operating_comparison"] = {
                "template_manifest_id": self.operating_template_id,
                "years": list(self.years or ()),
                "metric_groups": [
                    {
                        "name": group_name,
                        "metrics": [metric.to_payload() for metric in metrics],
                    }
                    for group_name, metrics in self.operating_sections
                ],
            }
        return payload


def normalize_comps_payload(
    payload: dict[str, Any],
    *,
    focal_ticker: str | None,
    kpi_values: dict[str, Any] | None,
) -> CompsGrid:
    """Validate and normalize an industry_peer_comparison payload for rendering."""

    parsed = CompsPayload.model_validate(payload)
    ticker_order = _ticker_order(parsed)
    if not ticker_order:
        raise ValueError("comps_payload must include at least one ticker in peers or metric values")

    focal = _normalize_ticker(focal_ticker) if focal_ticker else ticker_order[0]
    if focal not in ticker_order:
        raise ValueError(
            f"focal_ticker {focal!r} is not present in comps_payload tickers: {ticker_order}"
        )
    tickers = tuple([focal, *[ticker for ticker in ticker_order if ticker != focal]])
    normalized_kpis = _normalize_kpi_values(kpi_values or {})
    overlay_hits: set[tuple[str, str]] = set()
    warnings: list[str] = []

    sections: list[tuple[str, tuple[CompsMetricResult, ...]]] = []
    for section in parsed.sections:
        metrics: list[CompsMetricResult] = []
        for metric in section.metrics:
            source_values = {
                _normalize_ticker(ticker): cell for ticker, cell in metric.values.items()
            }
            values: dict[str, CompsMetricCell] = {}
            for ticker in tickers:
                cell = _cell_from_payload(source_values.get(ticker))
                overlay = _snapshot_overlay(normalized_kpis, ticker, metric.key)
                if cell.value is None and overlay is not _MISSING:
                    cell = CompsMetricCell(value=overlay, derived=True)
                    overlay_hits.add((ticker, metric.key))
                if cell.value is None:
                    warnings.append(
                        f"missing value: {section.name}/{metric.key} {ticker}"
                    )
                values[ticker] = cell
            metrics.append(
                CompsMetricResult(
                    key=metric.key,
                    label=metric.label,
                    units=metric.units,
                    values=values,
                    median=_median_cell(values.values(), metric.median),
                )
            )
        sections.append((section.name, tuple(metrics)))

    operating_sections: list[tuple[str, tuple[CompsMetricResult, ...]]] = []
    years: tuple[int, ...] | None = None
    operating_template_id: str | None = None
    operating = parsed.operating_comparison
    if operating is not None and operating.metric_groups:
        years = tuple(sorted(int(year) for year in operating.years))
        operating_template_id = operating.template_manifest_id
        for group in operating.metric_groups:
            group_metrics: list[CompsMetricResult] = []
            for metric in group.metrics:
                series: dict[str, dict[int, CompsMetricCell]] = {}
                source_series = {
                    _normalize_ticker(ticker): year_values
                    for ticker, year_values in metric.series.items()
                }
                for ticker in tickers:
                    source_years = source_series.get(ticker, {})
                    year_values: dict[int, CompsMetricCell] = {}
                    for year in years:
                        cell = _cell_from_payload(source_years.get(year))
                        overlay = _timeseries_overlay(
                            normalized_kpis, ticker, metric.key, year
                        )
                        if cell.value is None and overlay is not _MISSING:
                            cell = CompsMetricCell(value=overlay, derived=True)
                            overlay_hits.add((ticker, metric.key))
                        if cell.value is None:
                            warnings.append(
                                f"missing operating value: {group.name}/{metric.key} "
                                f"{ticker} {year}"
                            )
                        year_values[year] = cell
                    series[ticker] = year_values
                group_metrics.append(
                    CompsMetricResult(
                        key=metric.key,
                        label=metric.label,
                        units=metric.units,
                        series=series,
                        median_series={
                            year: _median_cell(
                                (series[ticker][year] for ticker in tickers),
                                metric.median_series.get(year),
                            )
                            for year in years
                        },
                    )
                )
            operating_sections.append((group.name, tuple(group_metrics)))

    for ticker, metric_key in _iter_kpi_keys(normalized_kpis):
        if (ticker, metric_key) not in overlay_hits:
            warnings.append(f"unused kpi_values entry: {ticker}.{metric_key}")

    return CompsGrid(
        template_id=parsed.template_manifest_id,
        industry_key=parsed.industry_key,
        as_of=parsed.as_of,
        tickers=tickers,
        focal_ticker=focal,
        sections=tuple(sections),
        operating_template_id=operating_template_id,
        years=years,
        operating_sections=tuple(operating_sections),
        warnings=tuple(warnings),
    )


def render_comps_plan(grid: CompsGrid, *, sheet_name: str) -> RenderPlan:
    if grid.is_operating:
        return _render_operating_plan(grid, sheet_name=sheet_name)
    return _render_snapshot_plan(grid, sheet_name=sheet_name)


def render_rows(grid: CompsGrid) -> list[list[Any]]:
    if grid.is_operating:
        return _render_operating_rows(grid)
    return _render_snapshot_rows(grid)


def comps_grid_payload(grid: CompsGrid) -> dict[str, Any]:
    return grid.to_payload()


def _render_snapshot_plan(grid: CompsGrid, *, sheet_name: str) -> RenderPlan:
    writes: list[CellWrite] = []
    formats: list[CellFormat] = []
    last_ticker_col = _column(2 + len(grid.tickers))
    median_col = _column(3 + len(grid.tickers))

    def write(cell: str, value: Any) -> None:
        writes.append(CellWrite(sheet=sheet_name, cell=cell, value=value))

    write("B1", "Comparable Companies")
    write("B4", "Metric")
    for index, ticker in enumerate(grid.tickers, start=3):
        write(f"{_column(index)}4", ticker)
    write(f"{median_col}4", "Median")

    row = 5
    data_row_end = row
    for section_name, metrics in grid.sections:
        write(f"B{row}", section_name)
        formats.append(
            CellFormat(sheet=sheet_name, range=f"B{row}:{median_col}{row}", bold=True)
        )
        row += 1
        for metric in metrics:
            write(f"B{row}", metric.label)
            for index, ticker in enumerate(grid.tickers, start=3):
                write(f"{_column(index)}{row}", metric.values[ticker].value)
            write(f"{median_col}{row}", f"=MEDIAN(C{row}:{last_ticker_col}{row})")
            number_format = _number_format(metric.units)
            if number_format is not None:
                formats.append(
                    CellFormat(
                        sheet=sheet_name,
                        range=f"C{row}:{median_col}{row}",
                        number_format=number_format,
                    )
                )
            data_row_end = row
            row += 1

    _write_footer(writes, sheet_name=sheet_name, row=row + 1, template_id=grid.template_id)
    formats.extend(
        [
            CellFormat(sheet=sheet_name, range="B1:B1", bold=True),
            CellFormat(sheet=sheet_name, range=f"B4:{median_col}4", bold=True),
            CellFormat(
                sheet=sheet_name,
                range=f"C4:C{max(data_row_end, 4)}",
                bold=True,
                fill_color=FOCAL_FILL_COLOR,
            ),
        ]
    )
    return RenderPlan(
        sheet_setups=[
            SheetSetup(
                sheet=sheet_name,
                column_widths=_snapshot_column_widths(median_col),
                freeze_panes=None,
                first_data_column="C",
            )
        ],
        writes=writes,
        formats=formats,
    )


def _render_operating_plan(grid: CompsGrid, *, sheet_name: str) -> RenderPlan:
    writes: list[CellWrite] = []
    formats: list[CellFormat] = []
    years = tuple(grid.years or ())
    last_year_col = _column(2 + len(years)) if years else "C"

    def write(cell: str, value: Any) -> None:
        writes.append(CellWrite(sheet=sheet_name, cell=cell, value=value))

    write("B1", "Operating Comparable Companies")
    write("B4", "Metric / Ticker")
    for index, year in enumerate(years, start=3):
        write(f"{_column(index)}4", year)

    row = 5
    for group_name, metrics in grid.operating_sections:
        write(f"B{row}", group_name)
        formats.append(
            CellFormat(sheet=sheet_name, range=f"B{row}:{last_year_col}{row}", bold=True)
        )
        row += 1
        for metric in metrics:
            write(f"B{row}", metric.label)
            formats.append(
                CellFormat(sheet=sheet_name, range=f"B{row}:{last_year_col}{row}", bold=True)
            )
            row += 1
            first_ticker_row = row
            for ticker in grid.tickers:
                write(f"B{row}", ticker)
                for index, year in enumerate(years, start=3):
                    write(f"{_column(index)}{row}", metric.series[ticker][year].value)
                if ticker == grid.focal_ticker:
                    formats.append(
                        CellFormat(
                            sheet=sheet_name,
                            range=f"B{row}:{last_year_col}{row}",
                            bold=True,
                            fill_color=FOCAL_FILL_COLOR,
                        )
                    )
                row += 1
            last_ticker_row = row - 1
            write(f"B{row}", "Median")
            for index, _year in enumerate(years, start=3):
                col = _column(index)
                write(f"{col}{row}", f"=MEDIAN({col}{first_ticker_row}:{col}{last_ticker_row})")
            number_format = _number_format(metric.units)
            if number_format is not None:
                formats.append(
                    CellFormat(
                        sheet=sheet_name,
                        range=f"C{first_ticker_row}:{last_year_col}{row}",
                        number_format=number_format,
                    )
                )
            formats.append(
                CellFormat(sheet=sheet_name, range=f"B{row}:{last_year_col}{row}", bold=True)
            )
            row += 1

    _write_footer(
        writes,
        sheet_name=sheet_name,
        row=row + 1,
        template_id=grid.operating_template_id or grid.template_id,
    )
    formats.extend(
        [
            CellFormat(sheet=sheet_name, range="B1:B1", bold=True),
            CellFormat(sheet=sheet_name, range=f"B4:{last_year_col}4", bold=True),
        ]
    )
    return RenderPlan(
        sheet_setups=[
            SheetSetup(
                sheet=sheet_name,
                column_widths=_operating_column_widths(last_year_col),
                freeze_panes=None,
                first_data_column="C",
            )
        ],
        writes=writes,
        formats=formats,
    )


def _render_snapshot_rows(grid: CompsGrid) -> list[list[Any]]:
    rows: list[list[Any]] = [
        [],
        ["", "Comparable Companies"],
        [],
        ["", "Metric", *grid.tickers, "Median"],
    ]
    for section_name, metrics in grid.sections:
        rows.append(["", section_name, *["" for _ in grid.tickers], ""])
        for metric in metrics:
            rows.append(
                [
                    "",
                    metric.label,
                    *[metric.values[ticker].value for ticker in grid.tickers],
                    metric.median.value if metric.median else None,
                ]
            )
    _append_footer_rows(rows, template_id=grid.template_id)
    return rows


def _render_operating_rows(grid: CompsGrid) -> list[list[Any]]:
    years = tuple(grid.years or ())
    rows: list[list[Any]] = [
        [],
        ["", "Operating Comparable Companies"],
        [],
        ["", "Metric / Ticker", *years],
    ]
    for group_name, metrics in grid.operating_sections:
        rows.append(["", group_name, *["" for _ in years]])
        for metric in metrics:
            rows.append(["", metric.label, *["" for _ in years]])
            for ticker in grid.tickers:
                rows.append(
                    [
                        "",
                        ticker,
                        *[metric.series[ticker][year].value for year in years],
                    ]
                )
            rows.append(
                [
                    "",
                    "Median",
                    *[
                        metric.median_series[year].value
                        if year in metric.median_series
                        else None
                        for year in years
                    ],
                ]
            )
    _append_footer_rows(rows, template_id=grid.operating_template_id or grid.template_id)
    return rows


def _ticker_order(payload: CompsPayload) -> list[str]:
    ordered: list[str] = []
    for peer in payload.peers:
        ticker = _normalize_ticker(peer.get("ticker") or peer.get("symbol"))
        if ticker and ticker not in ordered:
            ordered.append(ticker)
    for section in payload.sections:
        for metric in section.metrics:
            for ticker in metric.values:
                normalized = _normalize_ticker(ticker)
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
    operating = payload.operating_comparison
    if operating is not None:
        for group in operating.metric_groups:
            for metric in group.metrics:
                for ticker in metric.series:
                    normalized = _normalize_ticker(ticker)
                    if normalized and normalized not in ordered:
                        ordered.append(normalized)
    return ordered


def _cell_from_payload(cell: _PayloadCell | None) -> CompsMetricCell:
    if cell is None:
        return CompsMetricCell()
    return CompsMetricCell(
        value=cell.value,
        source_refs=tuple(str(ref) for ref in cell.source_refs),
        derived=bool(cell.derived),
    )


def _median_cell(
    cells: Any,
    payload_median: _PayloadCell | None,
) -> CompsMetricCell:
    values = [_numeric_value(cell.value) for cell in cells]
    numeric_values = [value for value in values if value is not None]
    if numeric_values:
        return CompsMetricCell(value=median(numeric_values), derived=True)
    return _cell_from_payload(payload_median)


def _numeric_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        converted = float(value)
        return converted if converted == converted else None
    if isinstance(value, str):
        text = value.strip().replace(",", "").replace("%", "")
        if not text:
            return None
        try:
            converted = float(text)
        except ValueError:
            return None
        return converted if converted == converted else None
    return None


class _Missing:
    pass


_MISSING = _Missing()


def _normalize_kpi_values(kpi_values: dict[str, Any]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for ticker, metrics in kpi_values.items():
        if not isinstance(metrics, dict):
            continue
        normalized[_normalize_ticker(ticker)] = {
            str(metric_key): value for metric_key, value in metrics.items()
        }
    return normalized


def _snapshot_overlay(
    kpi_values: dict[str, dict[str, Any]],
    ticker: str,
    metric_key: str,
) -> Any:
    value = kpi_values.get(ticker, {}).get(metric_key, _MISSING)
    if isinstance(value, dict):
        return _MISSING
    return value


def _timeseries_overlay(
    kpi_values: dict[str, dict[str, Any]],
    ticker: str,
    metric_key: str,
    year: int,
) -> Any:
    value = kpi_values.get(ticker, {}).get(metric_key, _MISSING)
    if isinstance(value, dict):
        return value.get(year, value.get(str(year), _MISSING))
    return value


def _iter_kpi_keys(kpi_values: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
    return [
        (ticker, metric_key)
        for ticker, metrics in sorted(kpi_values.items())
        for metric_key in sorted(metrics)
    ]


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _number_format(units: str | None) -> str | None:
    if units is None:
        return None
    return _NUMBER_FORMAT_BY_UNITS.get(units)


def _column(index: int) -> str:
    letters = ""
    value = int(index)
    while value:
        value, remainder = divmod(value - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters or "A"


def _snapshot_column_widths(median_col: str) -> dict[str, float]:
    widths = {"A": 3.0, "B": 28.0}
    for index in range(3, _column_index(median_col) + 1):
        widths[_column(index)] = 14.0
    return widths


def _operating_column_widths(last_year_col: str) -> dict[str, float]:
    widths = {"A": 3.0, "B": 28.0}
    for index in range(3, _column_index(last_year_col) + 1):
        widths[_column(index)] = 14.0
    return widths


def _column_index(column: str) -> int:
    value = 0
    for char in column.upper():
        value = value * 26 + ord(char) - 64
    return value


def _write_footer(
    writes: list[CellWrite],
    *,
    sheet_name: str,
    row: int,
    template_id: str | None,
) -> None:
    writes.append(
        CellWrite(
            sheet=sheet_name,
            cell=f"B{row}",
            value=f"Source: industry_peer_comparison engine; template: {template_id or ''}",
        )
    )
    writes.append(
        CellWrite(
            sheet=sheet_name,
            cell=f"B{row + 1}",
            value=f"Build date: {date.today().isoformat()}",
        )
    )


def _append_footer_rows(rows: list[list[Any]], *, template_id: str | None) -> None:
    rows.append([])
    rows.append(["", f"Source: industry_peer_comparison engine; template: {template_id or ''}"])
    rows.append(["", f"Build date: {date.today().isoformat()}"])


__all__ = [
    "CompsGrid",
    "CompsMetricCell",
    "CompsMetricResult",
    "CompsPayload",
    "comps_grid_payload",
    "normalize_comps_payload",
    "render_comps_plan",
    "render_rows",
]
