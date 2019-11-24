import os
import shutil

from contextlib import contextmanager
from tempfile import NamedTemporaryFile

from s3fs import S3FileSystem


class S3Downloader(object):
    def __init__(self, tmp_dir=None):
        key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

        self.tmp_dir = tmp_dir
        self.fs = S3FileSystem(key=key, secret=secret)

    @contextmanager
    def get(self, src):
        with self.fs.open(src) as s:
            with NamedTemporaryFile(dir=self.tmp_dir) as d:
                shutil.copyfileobj(s, d)
                d.flush()
                yield d.name
