import os
import requests
from contextlib import contextmanager
from tempfile import NamedTemporaryFile


class HTTPFS(object):
    def __init__(self, tmp_dir=None):
        session = requests.Session()

        user = os.environ.get("httpfs_username")
        password = os.environ.get("httpfs_password")

        if user is not None and password is not None:
            session.auth = (user, password)
        self.tmp_dir = tmp_dir

    def _copy(self, r, d, chunk_size=16 * 1024):
        for chunk in r.iter_content(chunk_size=chunk_size, decode_unicode=False):
            d.write(chunk)
        d.flush()

    @contextmanager
    def get(self, src):
        r = self.session.get(src, stream=True)
        with NamedTemporaryFile(dir=self.tmp_dir) as f:
            self._copy(r, f)
            yield f.name
