import os
import shutil

from contextlib import contextmanager
from tempfile import NamedTemporaryFile

from s3fs import S3FileSystem


class S3Downloader(object):
    def __init__(self, tmp_dir=None, chunk_size=16 * 1024, **kwargs):
        key = kwargs.pop("key", os.environ.get("AWS_ACCESS_KEY_ID"))
        secret = kwargs.pop("secret", os.environ.get("AWS_SECRET_ACCESS_KEY"))

        self.tmp_dir = tmp_dir
        self.chunk_size = chunk_size
        self.fs = S3FileSystem(key=key, secret=secret, **kwargs)

    @contextmanager
    def get(self, src):
        with self.fs.open(src) as s:
            with NamedTemporaryFile(dir=self.tmp_dir) as d:
                shutil.copyfileobj(s, d, length=self.chunk_size)
                d.flush()
                yield d.name
