import pytest

from insights_messaging.template import DefaultingTemplate as Template


def test_simple_substitution():
    # no subs
    assert Template("hello world").substitute() == "hello world"

    # simple subs
    assert Template("hello $who").substitute(who="world") == "hello world"
    assert Template("hello $who").substitute({"who": "world"}) == "hello world"

    # braced subs
    assert Template("hello ${who}").substitute(who="world") == "hello world"
    assert Template("hello ${who}").substitute({"who": "world"}) == "hello world"

    # escapes
    assert Template("hello $$who").substitute() == "hello $who"
    assert Template("hello $${who}").substitute() == "hello ${who}"

    # missing keys
    with pytest.raises(Exception):
        Template("hello $who").substitute()

    with pytest.raises(Exception):
        Template("hello ${who}").substitute()


def test_default_substitution():
    assert Template("hello ${who:world}").substitute() == "hello world"
    assert Template("hello ${who:w:orld}").substitute() == "hello w:orld"
    assert Template("hello ${who:}").substitute() == "hello "

    # defaults not allowed outside of braces
    with pytest.raises(Exception):
        assert Template("hello $who:world").substitute()


def test_simple_safe_substitution():
    assert Template("hello world").safe_substitute() == "hello world"

    assert Template("hello $who").safe_substitute(who="world") == "hello world"
    assert Template("hello $who").safe_substitute({"who": "world"}) == "hello world"

    assert Template("hello ${who}").safe_substitute(who="world") == "hello world"
    assert Template("hello ${who}").safe_substitute({"who": "world"}) == "hello world"

    assert Template("hello $who").safe_substitute() == "hello $who"
    assert Template("hello ${who}").safe_substitute() == "hello ${who}"


def test_default_safe_substitution():
    assert Template("hello ${who:world}").safe_substitute() == "hello world"
    assert Template("hello ${who:w:orld}").safe_substitute() == "hello w:orld"
