import os
import requests
from contextlib import contextmanager
from tempfile import NamedTemporaryFile


class Http(object):
    def __init__(self, tmp_dir=None, chunk_size=16 * 1024):
        session = requests.Session()

        user = os.environ.get("httpfs_username")
        password = os.environ.get("httpfs_password")

        if user is not None and password is not None:
            session.auth = (user, password)
        self.tmp_dir = tmp_dir
        self.chunk_size = chunk_size

    def _copy(self, r, d):
        for chunk in r.iter_content(chunk_size=self.chunk_size, decode_unicode=False):
            d.write(chunk)
        d.flush()

    @contextmanager
    def get(self, src):
        r = self.session.get(src, stream=True)
        r.raise_for_status()
        with NamedTemporaryFile(dir=self.tmp_dir) as d:
            self._copy(r, d)
            yield d.name
