"""YAML-based template engine with locale fallback."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Protocol


class Renderer(Protocol):
    """Interface for custom template renderers."""

    def render(self, key: str, locale: str, vars: dict[str, Any]) -> str: ...


class TemplateEngine:
    """YAML template engine with locale fallback chain.

    Template format (YAML)::

        welcome:
          en: "Hello, {name}!"
          ru: "Привет, {name}!"

    Locale fallback: exact → base (ru-RU → ru) → "en" → any.
    """

    def __init__(self) -> None:
        self._templates: dict[str, dict[str, str]] = {}

    def load_file(self, path: str) -> None:
        import yaml
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        self._load(data)

    def load_bytes(self, raw: bytes) -> None:
        import yaml
        data = yaml.safe_load(raw)
        self._load(data)

    def _load(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            raise ValueError("templates must be a YAML mapping")
        for key, locales in data.items():
            if not isinstance(locales, dict):
                raise ValueError(f"template '{key}' must map locale → text")
            self._templates[key] = {str(k): str(v) for k, v in locales.items()}

    def render(self, key: str, locale: str, vars: dict[str, Any]) -> str:
        locales = self._templates.get(key)
        if not locales:
            raise KeyError(f"template not found: {key}")

        template = self._resolve_locale(locales, locale)
        return template.format_map(_SafeDict(vars))

    def has_key(self, key: str) -> bool:
        return key in self._templates

    @staticmethod
    def _resolve_locale(locales: dict[str, str], locale: str) -> str:
        # Exact match
        if locale and locale in locales:
            return locales[locale]
        # Base locale (ru-RU → ru)
        if locale and "-" in locale:
            base = locale.split("-")[0]
            if base in locales:
                return locales[base]
        # Fallback to "en"
        if "en" in locales:
            return locales["en"]
        # Any available
        return next(iter(locales.values()))


class _SafeDict(dict):  # type: ignore[type-arg]
    """Dict that returns {key} for missing keys instead of raising."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
