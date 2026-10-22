"""Template metadata helpers."""



def build_sia_generic_template(*args, **kwargs):
    """Build the checked-in generic SIA template artifact."""

    from .template_builder import build_sia_generic_template as _build_sia_generic_template

    return _build_sia_generic_template(*args, **kwargs)


def load_sia_generic_template():
    """Load the checked-in generic SIA template artifact."""

    from .template_builder import load_sia_generic_template as _load_sia_generic_template

    return _load_sia_generic_template()


__all__ = [
    "build_sia_generic_template",
    "load_sia_generic_template",
]
