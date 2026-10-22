"""Standalone compatibility shims for financial-modeling-tools."""

from pkgutil import extend_path


__path__ = extend_path(__path__, __name__)

# When the platform API tree is also installed, participate in its canonical
# agent bootstrap regardless of which regular ``api`` package wins path order.
try:
  from ._agent_compatibility import install as _install_agent_compatibility
except ModuleNotFoundError as exc:
  if exc.name != "api._agent_compatibility":
    raise
else:
  code_freshness = _install_agent_compatibility(globals())
