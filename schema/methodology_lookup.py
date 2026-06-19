from __future__ import annotations

from .methodology import MethodologyUnit, SiaPhase, WikiArticle, WikiArticleType
from .methodology_loader import MethodologyIndex, WikiIndex
from .process_template import StrategyCategoryId


def find_methodology(
    *,
    index: MethodologyIndex,
    category: StrategyCategoryId | None = None,
    sia_phase: SiaPhase | None = None,
    concept: str | None = None,
    tag: str | None = None,
) -> list[MethodologyUnit]:
    """AND of all provided filters. None on a parameter = no constraint on that axis.
    Empty filter set (all None) returns all units. Returns units in deterministic
    order - sorted by `name` ascending.
    """
    candidate_sets: list[set[str]] = []

    if category is not None:
        candidate_sets.append(set(index.by_category(category)))
    if sia_phase is not None:
        candidate_sets.append(set(index.by_phase(sia_phase)))
    if concept is not None:
        candidate_sets.append(set(index.by_concept(concept)))
    if tag is not None:
        candidate_sets.append(set(index.by_tag(tag)))

    if candidate_sets:
        candidate_names = set.intersection(*candidate_sets)
    else:
        candidate_names = set(index.names())

    units = [unit for name in candidate_names if (unit := index.get(name)) is not None]
    return sorted(units, key=lambda unit: unit.name)


def get_methodology(*, index: MethodologyIndex, name: str) -> MethodologyUnit | None:
    """Single fetch by unit name. Returns None if not found."""
    return index.get(name)


def lookup_wiki(
    *,
    index: WikiIndex,
    article_type: WikiArticleType | None = None,
    source_module: int | None = None,
    slug: str | None = None,
) -> list[WikiArticle]:
    """AND of provided filters. Empty filter set returns all articles.
    Sorted by (type, slug) ascending. Note: when `slug` is provided,
    returns CROSS-TYPE matches (a slug can exist in multiple type dirs);
    callers narrow with `article_type` if they want a single type.
    """
    if slug is not None:
        articles = index.find_by_slug(slug)
    elif article_type is not None:
        articles = index.by_type(article_type)
    else:
        articles = index.all_articles()

    matches = []
    for article in articles:
        if article_type is not None and article.type != article_type:
            continue
        if slug is not None and article.slug != slug:
            continue
        if source_module is not None and source_module not in article.source_modules:
            continue
        matches.append(article)

    return sorted(matches, key=lambda article: (article.type, article.slug))


def get_wiki_article(
    *,
    index: WikiIndex,
    article_type: WikiArticleType,
    slug: str,
) -> WikiArticle | None:
    """Single fetch by (type, slug). Returns None if not found."""
    return index.get(article_type, slug)


__all__ = [
    "find_methodology",
    "get_methodology",
    "get_wiki_article",
    "lookup_wiki",
]
