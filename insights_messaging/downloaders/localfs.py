import os
from contextlib import contextmanager


class LocalFS(object):
    @contextmanager
    def get(self, src):
        path = os.path.realpath(os.path.expanduser(src))
        yield path
