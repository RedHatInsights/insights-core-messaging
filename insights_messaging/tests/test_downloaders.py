"""
Tests for downloader implementations.

Downloaders provide a context manager ``get(src)`` that yields a local
file path for the consumer to process.  These tests cover the local
filesystem downloader.
"""

import os
import tempfile

from insights_messaging.downloaders.localfs import LocalFS


# ---------------------------------------------------------------------------
# LocalFS tests
# ---------------------------------------------------------------------------

def test_localfs_yields_real_path():
    """Verify that LocalFS.get() yields the realpath of the source."""
    fs = LocalFS()
    with tempfile.NamedTemporaryFile() as f:
        with fs.get(f.name) as path:
            assert path == os.path.realpath(f.name), (
                "LocalFS.get() should yield the realpath of the source, "
                "got %r" % path
            )


def test_localfs_resolves_symlinks(tmp_path):
    """Verify that LocalFS.get() resolves symbolic links."""
    real_file = tmp_path / "real.txt"
    real_file.write_text("content")
    link = tmp_path / "link.txt"
    link.symlink_to(real_file)

    fs = LocalFS()
    with fs.get(str(link)) as path:
        assert path == str(real_file.resolve()), (
            "LocalFS.get() should resolve symlinks, got %r" % path
        )


def test_localfs_expands_user_home():
    """Verify that LocalFS.get() expands ~ in paths."""
    fs = LocalFS()
    with fs.get("~") as path:
        assert path == os.path.realpath(os.path.expanduser("~")), (
            "LocalFS.get() should expand ~ to the home directory"
        )


def test_localfs_context_manager_cleanup():
    """Verify that LocalFS.get() works as a proper context manager."""
    fs = LocalFS()
    with tempfile.NamedTemporaryFile() as f:
        with fs.get(f.name) as path:
            # Inside context: path should be valid.
            assert isinstance(path, str)
        # Outside context: no cleanup needed for LocalFS, but should
        # not raise.
