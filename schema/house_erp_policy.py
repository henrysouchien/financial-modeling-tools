from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class HouseErp:
    value: float
    source: str
    source_url: str
    rationale: str
    as_of: str


_DEFAULT_HOUSE_ERP = HouseErp(
    value=0.0423,
    source="Damodaran implied US equity risk premium (year-end 2025)",
    source_url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histimpl.html",
    rationale=(
        "House standing assumption for autonomous CAPM cost-of-equity; "
        "refresh to latest monthly Damodaran observation."
    ),
    as_of="2025-12",
)


def _erp_value_from_env() -> float:
    raw = os.environ.get("HOUSE_ERP_DECIMAL")
    if raw is None:
        return _DEFAULT_HOUSE_ERP.value
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "HOUSE_ERP_DECIMAL must be a decimal with 0 < value < 0.20; "
            f"got {raw!r}"
        ) from exc
    if not 0 < value < 0.20:
        raise ValueError(
            "HOUSE_ERP_DECIMAL must be a decimal with 0 < value < 0.20; "
            f"got {raw!r}"
        )
    return value


def _env_or_default(name: str, default: str) -> str:
    return os.environ.get(name, default)


def get_house_erp() -> HouseErp:
    return HouseErp(
        value=_erp_value_from_env(),
        source=_env_or_default("HOUSE_ERP_SOURCE", _DEFAULT_HOUSE_ERP.source),
        source_url=_env_or_default("HOUSE_ERP_SOURCE_URL", _DEFAULT_HOUSE_ERP.source_url),
        rationale=_env_or_default("HOUSE_ERP_RATIONALE", _DEFAULT_HOUSE_ERP.rationale),
        as_of=_env_or_default("HOUSE_ERP_AS_OF", _DEFAULT_HOUSE_ERP.as_of),
    )


__all__ = ["HouseErp", "get_house_erp"]
