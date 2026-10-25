from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

from schema.provider_port.adapters.fmp.transport import provider_symbol


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
  """Whether the EDGAR metric-series endpoint at `base_url` declares an
  `axis_key` query parameter, read from the service's published OpenAPI
  schema. Issuer-free: no metric request is issued, so the probe needs no
  ticker."""
  parts = urllib_module.parse.urlsplit(base_url)
  schema_url = urllib_module.parse.urlunsplit(
    (parts.scheme, parts.netloc, "/openapi.json", "", "")
  )
  request = urllib_module.request.Request(
    schema_url,
    headers={
      "User-Agent": "model-engine-mcp",
      "Authorization": f"Bearer {api_key}",
    },
  )
  try:
    # A reachability probe answers now or not at all; a slow probe reads as no
    # axis-filter support, and no tool call waits on it.
    with urllib_module.request.urlopen(request, timeout=5) as response:
      payload = json_module.loads(response.read().decode("utf-8"))
  except Exception as exc:
    logging_module.info(
      "edgar_fetcher axis_key capability probe failed | exception_type=%s",
      type(exc).__name__,
    )
    return False

  if not isinstance(payload, dict):
    return False
  paths = payload.get("paths")
  operation = paths.get(parts.path) if isinstance(paths, dict) else None
  get_operation = operation.get("get") if isinstance(operation, dict) else None
  parameters = get_operation.get("parameters") if isinstance(get_operation, dict) else None
  if not isinstance(parameters, list):
    return False
  return any(
    isinstance(parameter, dict) and parameter.get("name") == "axis_key"
    for parameter in parameters
  )


def _request_failure_cause(exc: Exception) -> str:
  """Name the transport cause without echoing exception text, which can carry
  request credentials."""
  if isinstance(exc, urllib.error.HTTPError):
    return f"HTTPError {exc.code}"
  if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, BaseException):
    return f"URLError({type(exc.reason).__name__})"
  return type(exc).__name__


class _MetricSeriesRequest(Protocol):
  def __call__(self, params: dict) -> dict: ...


class _EdgarMetricSeriesFetcher:
  def __init__(
    self,
    request_metric_series: _MetricSeriesRequest,
    *,
    supports_axis_filter: bool,
  ) -> None:
    self._request_metric_series = request_metric_series
    self.supports_axis_filter = supports_axis_filter

  def __call__(
    self,
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
    }
    if axis_key is not None:
      params["axis_key"] = axis_key

    try:
      return self._request_metric_series(params)
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
        "error": f"EDGAR concept request failed: {_request_failure_cause(exc)}",
      }


def make_edgar_fetcher(
  *,
  probe_axis_filter_support_fn=probe_edgar_axis_filter_support,
  os_module=os,
  urllib_module=urllib,
  json_module=json,
  logging_module=logging,
) -> _EdgarMetricSeriesFetcher:
  api_key = os_module.getenv("EDGAR_API_KEY", "")
  base_url = (
    os_module.getenv("EDGAR_API_URL", "https://www.edgarparser.com").rstrip("/")
    + "/api/metric/series"
  )
  supports_axis_filter = probe_axis_filter_support_fn(base_url, api_key)
  logging_module.info("edgar_fetcher supports_axis_filter=%s", supports_axis_filter)

  def _request_metric_series(params: dict) -> dict:
    url = f"{base_url}?{urllib_module.parse.urlencode(params)}"
    request = urllib_module.request.Request(
      url,
      headers={
        "User-Agent": "model-engine-mcp",
        "Authorization": f"Bearer {api_key}",
      },
    )
    with urllib_module.request.urlopen(request) as response:
      payload = json_module.loads(response.read().decode("utf-8"))
      if not isinstance(payload, dict):
        raise ValueError("EDGAR API response was not a JSON object")
      return payload

  return _EdgarMetricSeriesFetcher(
    _request_metric_series,
    supports_axis_filter=supports_axis_filter,
  )


__all__ = [
  "apply_override_directory_env",
  "make_edgar_fetcher",
  "probe_edgar_axis_filter_support",
  "validate_file_path",
]
