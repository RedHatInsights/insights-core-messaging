"""
Tests for DefaultingTemplate — a string.Template subclass with default values.

DefaultingTemplate extends Python's string.Template to support
``${var:default}`` syntax used in YAML configuration files.  This allows
operators to provide fallback values for optional settings without
requiring every environment variable to be defined.
"""

import pytest

from insights_messaging.template import DefaultingTemplate as Template


def test_simple_substitution():
    """DefaultingTemplate.substitute() must handle standard template syntax.

    Tests: plain text (no substitution), $var and ${var} syntax, $$
    escape sequences, and KeyError on missing keys.  These behaviors
    are inherited from string.Template but must be preserved by the
    subclass.
    """
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
    with pytest.raises(KeyError):
        Template("hello $who").substitute()

    with pytest.raises(KeyError):
        Template("hello ${who}").substitute()


def test_default_substitution():
    """DefaultingTemplate must support ${var:default} syntax for fallback values.

    This is the key extension over string.Template.  When the variable
    is not provided, the default value (everything after the first colon
    up to the closing brace) is used.  Colons within the default are
    preserved.  Defaults are NOT allowed outside braces ($var:default
    must raise KeyError).
    """
    assert Template("hello ${who:world}").substitute() == "hello world"
    assert Template("hello ${who:w:orld}").substitute() == "hello w:orld"
    assert Template("hello ${who:}").substitute() == "hello "

    # defaults not allowed outside of braces
    with pytest.raises(KeyError):
        Template("hello $who:world").substitute()


def test_simple_safe_substitution():
    """safe_substitute() must leave unresolved variables in place rather than raising.

    This is used when partial substitution is acceptable — e.g. during
    config validation where not all env vars may be available yet.
    """
    assert Template("hello world").safe_substitute() == "hello world"

    assert Template("hello $who").safe_substitute(who="world") == "hello world"
    assert Template("hello $who").safe_substitute({"who": "world"}) == "hello world"

    assert Template("hello ${who}").safe_substitute(who="world") == "hello world"
    assert Template("hello ${who}").safe_substitute({"who": "world"}) == "hello world"

    assert Template("hello $who").safe_substitute() == "hello $who"
    assert Template("hello ${who}").safe_substitute() == "hello ${who}"


def test_default_safe_substitution():
    """safe_substitute() must still apply defaults from ${var:default} syntax.

    Even in safe mode, if a default is provided via the colon syntax,
    it should be used when the variable is missing.
    """
    assert Template("hello ${who:world}").safe_substitute() == "hello world"
    assert Template("hello ${who:w:orld}").safe_substitute() == "hello w:orld"
