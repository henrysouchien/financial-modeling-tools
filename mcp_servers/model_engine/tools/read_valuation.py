from __future__ import annotations

from copy import deepcopy
from typing import Any


def model_quality_readiness_for_valuation_summary(
  *,
  deps: Any,
  file_path: str,
  historical_cutoff_year: int | None,
  valuation_input_readiness: dict[str, Any],
  merge_valuation_input_readiness_fn: Any | None = None,
) -> dict[str, Any]:
  try:
    summary_result = deps.summarize(
      file_path,
      historical_cutoff_year=historical_cutoff_year,
      include_items=False,
    )
    readiness = summary_result.get("model_quality_readiness")
  except Exception as exc:
    readiness = {
      "status": "incomplete",
      "scope": "model_quality",
      "projection_periods": [],
      "domains": {},
      "issues": [
        {
          "code": "model_quality_readiness_unavailable",
          "severity": "warning",
          "domain": "valuation",
          "detail": (
            "model_summarize could not compute model_quality_readiness "
            f"for this valuation summary: {exc}"
          ),
        }
      ],
      "summary": "incomplete: model_quality_readiness unavailable",
    }
  if not isinstance(readiness, dict):
    readiness = {
      "status": "incomplete",
      "scope": "model_quality",
      "projection_periods": [],
      "domains": {},
      "issues": [
        {
          "code": "model_quality_readiness_unavailable",
          "severity": "warning",
          "domain": "valuation",
          "detail": "model_summarize did not return model_quality_readiness",
        }
      ],
      "summary": "incomplete: model_quality_readiness unavailable",
    }
  merge_fn = merge_valuation_input_readiness_fn or merge_valuation_input_readiness
  return merge_fn(readiness, valuation_input_readiness)


def merge_valuation_input_readiness(
  readiness: dict[str, Any],
  valuation_input_readiness: dict[str, Any],
  *,
  valuation_input_quality_issue_fn: Any | None = None,
  upsert_quality_issue_fn: Any | None = None,
  quality_status_fn: Any | None = None,
  model_quality_summary_fn: Any | None = None,
) -> dict[str, Any]:
  input_issue_fn = valuation_input_quality_issue_fn or valuation_input_quality_issue
  upsert_issue_fn = upsert_quality_issue_fn or upsert_quality_issue
  status_fn = quality_status_fn or quality_status
  summary_fn = model_quality_summary_fn or model_quality_summary
  result = deepcopy(readiness)
  result.setdefault("scope", "model_quality")
  result.setdefault("projection_periods", [])
  domains = result.setdefault("domains", {})
  if not isinstance(domains, dict):
    domains = {}
    result["domains"] = domains
  issues = result.setdefault("issues", [])
  if not isinstance(issues, list):
    issues = []
    result["issues"] = issues

  input_issue = input_issue_fn(valuation_input_readiness)
  if input_issue is not None:
    valuation_domain = domains.get("valuation")
    if not isinstance(valuation_domain, dict):
      valuation_domain = {
        "status": "ready",
        "required_items": [
          "tpl.v.current_valuation.stock_price",
          "tpl.v.current_valuation.shares_outstanding",
          "tpl.v.current_valuation.net_debt",
          "tpl.v.dcf.dcf_price",
        ],
        "missing_periods": [],
        "issues": [],
      }
      domains["valuation"] = valuation_domain
    domain_issues = valuation_domain.get("issues")
    if not isinstance(domain_issues, list):
      domain_issues = []
      valuation_domain["issues"] = domain_issues
    upsert_issue_fn(domain_issues, input_issue)
    upsert_issue_fn(issues, input_issue)
    valuation_domain["status"] = status_fn(domain_issues)

    result["status"] = status_fn(
      issues,
      fallback=str(result.get("status") or "unknown"),
    )
    result["summary"] = summary_fn(result["status"], domains)
    return result

  status = str(result.get("status") or "unknown")
  if status not in {"ready", "incomplete", "blocked", "unknown"}:
    status = status_fn(issues, fallback="unknown")
    result["status"] = status
  if not isinstance(result.get("summary"), str) or not result.get("summary"):
    result["summary"] = summary_fn(status, domains)
  return result


def valuation_input_quality_issue(
  valuation_input_readiness: dict[str, Any],
) -> dict[str, Any] | None:
  if valuation_input_readiness.get("status") != "incomplete":
    return None
  missing = [str(item) for item in valuation_input_readiness.get("missing") or []]
  severity = "blocking" if missing else "warning"
  detail = (
    f"valuation_input_readiness is incomplete; missing inputs: {', '.join(missing)}"
    if missing
    else "valuation_input_readiness is incomplete; only placeholder/staleness flags may be present"
  )
  return {
    "code": "valuation_inputs_incomplete",
    "severity": severity,
    "domain": "valuation",
    "detail": detail,
    "item_id": None,
    "missing_periods": [],
    "related_item_ids": missing,
  }


def upsert_quality_issue(issues: list[Any], issue: dict[str, Any]) -> None:
  key = (issue.get("domain"), issue.get("code"))
  for index, existing in enumerate(issues):
    if (
      isinstance(existing, dict)
      and (existing.get("domain"), existing.get("code")) == key
    ):
      issues[index] = issue
      return
  issues.append(issue)


def quality_status(issues: list[Any], *, fallback: str = "ready") -> str:
  fallback = fallback if fallback in {"ready", "incomplete", "blocked", "unknown"} else "unknown"
  if any(
    isinstance(issue, dict) and issue.get("severity") == "blocking"
    for issue in issues
  ) or fallback == "blocked":
    return "blocked"
  if issues or fallback == "incomplete":
    return "incomplete"
  return fallback


def model_quality_summary(status: str, domains: dict[str, Any]) -> str:
  if status == "ready":
    return "share count, working capital, valuation, and segment-basis quality checks are ready"
  pieces = [
    f"{domain}={readiness.get('status')}"
    for domain, readiness in domains.items()
    if isinstance(readiness, dict) and readiness.get("status") != "ready"
  ]
  return f"{status}: " + ", ".join(pieces) if pieces else f"{status}: model quality readiness unavailable"


__all__ = [
  "merge_valuation_input_readiness",
  "model_quality_readiness_for_valuation_summary",
  "model_quality_summary",
  "quality_status",
  "upsert_quality_issue",
  "valuation_input_quality_issue",
]
