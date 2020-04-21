import os
import time
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import requests
from prometheus_client import Summary

PREFIX = __name__.replace(".", "_")

GET_TIME = Summary(f"{PREFIX}_get_time", "Time spent in get()")
COPY_TIME = Summary(f"{PREFIX}_copy_time", "Time spent copying to tempfile")


class Http:
    def __init__(self, tmp_dir=None, chunk_size=16 * 1024):
        self.session = requests.Session()

        user = os.environ.get("httpfs_username")
        password = os.environ.get("httpfs_password")

        if user is not None and password is not None:
            self.session.auth = (user, password)
        self.tmp_dir = tmp_dir
        self.chunk_size = chunk_size

    @COPY_TIME.time()
    def _copy(self, r, d):
        for chunk in r.iter_content(chunk_size=self.chunk_size, decode_unicode=False):
            d.write(chunk)
        d.flush()

    @contextmanager
    def get(self, src):
        start = time.time()
        r = self.session.get(src, stream=True)
        GET_TIME.observe(time.time() - start)
        r.raise_for_status()
        with NamedTemporaryFile(dir=self.tmp_dir) as d:
            self._copy(r, d)
            yield d.name
