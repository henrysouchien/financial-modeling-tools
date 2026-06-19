from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .methodology import MethodologyUnit, WikiArticle


def parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Split a markdown file into (frontmatter_dict, body_md)."""
    if not text.startswith("---\n"):
        raise ValueError("file missing YAML frontmatter")

    parts = text.split("---\n", 2)
    if len(parts) < 3:
        raise ValueError("frontmatter not terminated")

    frontmatter = yaml.safe_load(parts[1])
    if frontmatter is None:
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        raise ValueError("YAML frontmatter must be a mapping")

    return frontmatter, parts[2]


def load_methodology_unit(path: Path) -> MethodologyUnit:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = parse_frontmatter(text)
    if "version" in frontmatter and not isinstance(frontmatter["version"], str):
        frontmatter["version"] = str(frontmatter["version"])
    frontmatter["body_md"] = body
    unit = MethodologyUnit(**frontmatter)
    if unit.name != path.stem:
        raise ValueError(f"name {unit.name!r} != filename stem {path.stem!r}")
    return unit


def load_wiki_article(path: Path) -> WikiArticle:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = parse_frontmatter(text)
    frontmatter["body_md"] = body
    frontmatter["slug"] = path.stem
    return WikiArticle(**frontmatter)


class MethodologyIndex:
    """Index of MethodologyUnit objects keyed by name. Singleton-friendly."""

    LEGACY_EXCLUDE = frozenset({"investment_process", "black_book", "software_valuation"})

    def __init__(self, root: Path):
        self._root = root
        self._by_name: dict[str, MethodologyUnit] = {}
        self._by_phase: dict[str, list[str]] = {}
        self._by_category: dict[str, list[str]] = {}
        self._by_concept: dict[str, list[str]] = {}
        self._by_tag: dict[str, list[str]] = {}
        self.load(root)

    def load(self, root: Path) -> None:
        self._by_name.clear()
        self._by_phase.clear()
        self._by_category.clear()
        self._by_concept.clear()
        self._by_tag.clear()

        for path in root.rglob("*.md"):
            if "wiki" in path.parts:
                continue
            if path.stem.startswith("_"):
                continue
            if path.parent == root and path.stem in self.LEGACY_EXCLUDE:
                continue
            try:
                unit = load_methodology_unit(path)
            except Exception as exc:
                raise ValueError(f"{path}: {exc}") from exc

            self._by_name[unit.name] = unit
            self._by_phase.setdefault(unit.sia_phase, []).append(unit.name)
            for category in unit.process_template_categories:
                self._by_category.setdefault(category, []).append(unit.name)
            for concept in unit.concepts:
                self._by_concept.setdefault(concept, []).append(unit.name)
            for tag in unit.methodology_tags:
                self._by_tag.setdefault(tag, []).append(unit.name)

    def get(self, name: str) -> MethodologyUnit | None:
        return self._by_name.get(name)

    def names(self) -> list[str]:
        return sorted(self._by_name.keys())

    def by_phase(self, phase: str) -> list[str]:
        return list(self._by_phase.get(phase, []))

    def by_category(self, category: str) -> list[str]:
        return list(self._by_category.get(category, []))

    def by_concept(self, concept: str) -> list[str]:
        return list(self._by_concept.get(concept, []))

    def by_tag(self, tag: str) -> list[str]:
        return list(self._by_tag.get(tag, []))


class WikiIndex:
    """Index of WikiArticle objects keyed by (type, slug)."""

    def __init__(self, root: Path):
        self._root = root
        self._by_type_slug: dict[tuple[str, str], WikiArticle] = {}
        self._by_slug: dict[str, list[WikiArticle]] = {}
        self._by_source_module: dict[int, list[tuple[str, str]]] = {}
        self.load(root)

    def load(self, root: Path) -> None:
        self._by_type_slug.clear()
        self._by_slug.clear()
        self._by_source_module.clear()

        wiki_root = root / "wiki"
        if not wiki_root.exists():
            return

        for path in wiki_root.rglob("*.md"):
            if path.stem.startswith("_"):
                continue
            relative_path = path.relative_to(wiki_root)
            if len(relative_path.parts) >= 3 and relative_path.parts[:2] == ("patterns", "external"):
                continue
            try:
                article = load_wiki_article(path)
            except Exception as exc:
                raise ValueError(f"{path}: {exc}") from exc

            key = (article.type, article.slug)
            self._by_type_slug[key] = article
            self._by_slug.setdefault(article.slug, []).append(article)
            for module in article.source_modules:
                self._by_source_module.setdefault(module, []).append(key)

    def get(self, article_type: str, slug: str) -> WikiArticle | None:
        return self._by_type_slug.get((article_type, slug))

    def find_by_slug(self, slug: str) -> list[WikiArticle]:
        """Cross-type lookup for related slugs that can point to any wiki type."""
        return list(self._by_slug.get(slug, []))

    def by_type(self, article_type: str) -> list[WikiArticle]:
        return sorted(
            (
                article
                for (type_, _slug), article in self._by_type_slug.items()
                if type_ == article_type
            ),
            key=lambda article: article.slug,
        )

    def by_source_module(self, module: int) -> list[tuple[str, str]]:
        return list(self._by_source_module.get(module, []))

    def all_articles(self) -> list[WikiArticle]:
        return sorted(
            self._by_type_slug.values(),
            key=lambda article: (article.type, article.slug),
        )


__all__ = [
    "MethodologyIndex",
    "WikiIndex",
    "load_methodology_unit",
    "load_wiki_article",
    "parse_frontmatter",
]
