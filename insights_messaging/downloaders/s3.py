import shutil

from contextlib import contextmanager
from tempfile import NamedTemporaryFile

from s3fs import S3FileSystem


class S3Downloader(object):
    def __init__(self, tmp_dir=None, chunk_size=16 * 1024, **kwargs):
        self.tmp_dir = tmp_dir
        self.chunk_size = chunk_size
        self.fs = S3FileSystem(**kwargs)

    @contextmanager
    def get(self, src):
        with self.fs.open(src) as s:
            with NamedTemporaryFile(dir=self.tmp_dir) as d:
                shutil.copyfileobj(s, d, length=self.chunk_size)
                d.flush()
                yield d.name
