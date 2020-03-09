from collections import ChainMap as _ChainMap
from string import Template


def _infer_type(v):
    if not v:
        return

    if v.lower() in ["true", "false"]:
        return bool(v)

    for conv in [int, float]:
        try:
            return conv(v)
        except:
            pass

    return v


class DefaultingTemplate(Template):
    """
    Subclass of Template that allows default values in a braced substitution.

    Examples:
        $EXAMPLE
        ${EXAMPLE}
        ${EXAMPLE:some default}
    """

    pattern = r"""
    \$(?:
      (?P<escaped>\$)                       |   # Escape sequence of two delimiters
      (?P<named>([_a-z][_a-z0-9]*))         |   # delimiter and a Python identifier
      {(?P<braced>([^}]*))}                 |   # delimiter and a braced identifier
      (?P<invalid>)                             # Other ill-formed delimiter exprs
    )
    """

    def extract_value(self, mapping, named):
        if ":" not in named:
            return str(mapping[named])
        key, default = named.split(":", 1)
        return str(mapping.get(key, default))

    def substitute(*args, **kws):
        if not args:
            raise TypeError("descriptor 'substitute' of 'Template' object "
                            "needs an argument")
        self, *args = args  # allow the "self" keyword be passed
        if len(args) > 1:
            raise TypeError('Too many positional arguments')
        if not args:
            mapping = kws
        elif kws:
            mapping = _ChainMap(kws, args[0])
        else:
            mapping = args[0]

        # Helper function for .sub()
        def convert(mo):
            # Check the most common path first.
            named = mo.group('named') or mo.group('braced')
            if named is not None:
                return self.extract_value(mapping, named)
            if mo.group('escaped') is not None:
                return self.delimiter
            if mo.group('invalid') is not None:
                self._invalid(mo)
            raise ValueError('Unrecognized named group in pattern',
                             self.pattern)
        return _infer_type(self.pattern.sub(convert, self.template))

    def safe_substitute(*args, **kws):
        if not args:
            raise TypeError("descriptor 'safe_substitute' of 'Template' object "
                            "needs an argument")
        self, *args = args  # allow the "self" keyword be passed
        if len(args) > 1:
            raise TypeError('Too many positional arguments')
        if not args:
            mapping = kws
        elif kws:
            mapping = _ChainMap(kws, args[0])
        else:
            mapping = args[0]

        # Helper function for .sub()
        def convert(mo):
            named = mo.group('named') or mo.group('braced')
            if named is not None:
                try:
                    return self.extract_value(mapping, named)
                except KeyError:
                    return mo.group()
            if mo.group('escaped') is not None:
                return self.delimiter
            if mo.group('invalid') is not None:
                return mo.group()
            raise ValueError('Unrecognized named group in pattern',
                             self.pattern)
        return _infer_type(self.pattern.sub(convert, self.template))
