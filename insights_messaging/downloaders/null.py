from contextlib import contextmanager


class NullDownloader:
    @contextmanager
    def get(self, src):
        yield src  # Do nothing.  Defer to the extractor
