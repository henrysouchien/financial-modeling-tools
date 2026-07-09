from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from api.research.source_html import provider_symbol


def apply_override_directory_env(
  *,
  overrides_dir_env: str,
  schema_overrides_module: Any,
  os_module=os,
  path_cls=Path,
) -> Path | None:
  configured = os_module.environ.get(overrides_dir_env, "").strip()
  if not configured:
    return None
  override_dir = path_cls(configured).expanduser().resolve(strict=False)
  schema_overrides_module.DEFAULT_OVERRIDES_DIR = override_dir
  return override_dir


def validate_file_path(file_path: str, *, os_path=os.path) -> str:
  resolved = os_path.expanduser(file_path)
  if not os_path.isfile(resolved):
    raise FileNotFoundError(f"Model file not found: {resolved}")
  return resolved


def probe_edgar_axis_filter_support(
  base_url: str,
  api_key: str,
  *,
  urllib_module=urllib,
  json_module=json,
  logging_module=logging,
) -> bool:
  axis_key = "__probe__"
  params = {
    "ticker": "AAPL",
    "metric_name": "RevenueFromContractWithCustomerExcludingAssessedTax",
    "end_year": 2024,
    "end_quarter": 4,
    "periods": 1,
    "full_year_mode": "true",
    "include_equivalents": "false",
    "axis_key": axis_key,
    "key": api_key,
  }
  url = f"{base_url}?{urllib_module.parse.urlencode(params)}"
  request = urllib_module.request.Request(url, headers={"User-Agent": "model-engine-mcp"})
  try:
    with urllib_module.request.urlopen(request, timeout=5) as response:
      payload = json_module.loads(response.read().decode("utf-8"))
  except urllib_module.error.HTTPError as exc:
    try:
      payload = json_module.loads(exc.read().decode("utf-8"))
    except Exception:
      logging_module.info("edgar_fetcher axis_key capability probe failed: %s", exc)
      return False
  except Exception as exc:
    logging_module.info("edgar_fetcher axis_key capability probe failed: %s", exc)
    return False

  if not isinstance(payload, dict):
    return False
  if payload.get("axis_key") == axis_key:
    return True
  if payload.get("error_type") == "axis_key_not_found":
    return True
  series = payload.get("series")
  if isinstance(series, list) and any(
    isinstance(entry, dict) and entry.get("error_type") == "axis_key_not_found"
    for entry in series
  ):
    return True
  return False


def make_edgar_fetcher(
  *,
  probe_axis_filter_support_fn=probe_edgar_axis_filter_support,
  os_module=os,
  urllib_module=urllib,
  json_module=json,
  logging_module=logging,
):
  api_key = os_module.getenv("EDGAR_API_KEY", "")
  base_url = (
    os_module.getenv("EDGAR_API_URL", "https://www.edgarparser.com").rstrip("/")
    + "/api/metric/series"
  )
  supports_axis_filter = probe_axis_filter_support_fn(base_url, api_key)
  logging_module.info("edgar_fetcher supports_axis_filter=%s", supports_axis_filter)

  def _request_metric_series(params: dict, *, timeout: int) -> dict:
    url = f"{base_url}?{urllib_module.parse.urlencode(params)}"
    request = urllib_module.request.Request(url, headers={"User-Agent": "model-engine-mcp"})
    with urllib_module.request.urlopen(request, timeout=timeout) as response:
      payload = json_module.loads(response.read().decode("utf-8"))
      if not isinstance(payload, dict):
        raise ValueError("EDGAR API response was not a JSON object")
      return payload

  def _fetcher(
    ticker: str,
    metric_name: str,
    end_year: int,
    n_periods: int,
    *,
    include_equivalents: bool = False,
    axis_key: str | None = None,
  ) -> dict:
    params = {
      "ticker": provider_symbol(ticker),
      "metric_name": metric_name,
      "end_year": int(end_year),
      "end_quarter": 4,
      "periods": int(n_periods),
      "full_year_mode": "true",
      "include_equivalents": "true" if include_equivalents else "false",
      "key": api_key,
    }
    if axis_key is not None:
      params["axis_key"] = axis_key

    try:
      return _request_metric_series(params, timeout=120)
    except Exception as exc:
      return {
        "status": "error",
        "ticker": ticker,
        "metric_name": metric_name,
        "series": [],
        "periods_requested": int(n_periods),
        "periods_returned": 0,
        "periods_missing": 0,
        "periods_failed": int(n_periods),
        "error": str(exc),
      }

  _fetcher.supports_axis_filter = supports_axis_filter
  return _fetcher


__all__ = [
  "apply_override_directory_env",
  "make_edgar_fetcher",
  "probe_edgar_axis_filter_support",
  "validate_file_path",
]
