import pytest
from telecast._templates import TemplateEngine

_YAML = """
welcome:
  en: "Hello, {name}!"
  ru: "Привет, {name}!"
  ru-RU: "Здравствуй, {name}!"
farewell:
  en: "Bye!"
""".encode("utf-8")


@pytest.fixture
def engine():
    te = TemplateEngine()
    te.load_bytes(_YAML)
    return te


def test_exact_locale(engine: TemplateEngine):
    assert engine.render("welcome", "en", {"name": "Alice"}) == "Hello, Alice!"


def test_exact_locale_ru(engine: TemplateEngine):
    result = engine.render("welcome", "ru", {"name": "Bob"})
    assert result == "\u041f\u0440\u0438\u0432\u0435\u0442, Bob!"


def test_sub_locale(engine: TemplateEngine):
    result = engine.render("welcome", "ru-RU", {"name": "Ivan"})
    assert "\u0417\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439" in result


def test_fallback_to_base(engine: TemplateEngine):
    # ru-UA -> ru (base)
    result = engine.render("welcome", "ru-UA", {"name": "X"})
    assert "\u041f\u0440\u0438\u0432\u0435\u0442" in result


def test_fallback_to_en(engine: TemplateEngine):
    assert engine.render("welcome", "fr", {"name": "Y"}) == "Hello, Y!"


def test_missing_key_raises(engine: TemplateEngine):
    with pytest.raises(KeyError):
        engine.render("nonexistent", "en", {})


def test_missing_var_preserved(engine: TemplateEngine):
    result = engine.render("welcome", "en", {})
    assert result == "Hello, {name}!"


def test_has_key(engine: TemplateEngine):
    assert engine.has_key("welcome") is True
    assert engine.has_key("missing") is False
