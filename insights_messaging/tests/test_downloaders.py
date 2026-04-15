"""
Tests for downloader implementations.

Downloaders provide a context manager ``get(src)`` that yields a local
file path for the consumer to process.  These tests cover the local
filesystem, HTTP, and S3 downloaders.
"""

import os
import tempfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from insights_messaging.downloaders.httpfs import Http
from insights_messaging.downloaders.localfs import LocalFS
from insights_messaging.downloaders.s3 import S3Downloader

# ---------------------------------------------------------------------------
# LocalFS tests
# ---------------------------------------------------------------------------


def test_localfs_yields_real_path():
    """Verify that LocalFS.get() yields the realpath of the source."""
    fs = LocalFS()
    with tempfile.NamedTemporaryFile() as f, fs.get(f.name) as path:
        assert path == os.path.realpath(f.name), (
            f"LocalFS.get() should yield the realpath of the source, got {path!r}"
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
            f"LocalFS.get() should resolve symlinks, got {path!r}"
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
    with tempfile.NamedTemporaryFile() as f, fs.get(f.name) as path:
        # Inside context: path should be valid.
        assert isinstance(path, str)
        # Outside context: no cleanup needed for LocalFS, but should
        # not raise.


# ---------------------------------------------------------------------------
# Http downloader tests
# ---------------------------------------------------------------------------


@patch.dict(os.environ, {}, clear=True)
def test_http_no_auth_by_default():
    """Verify that Http does not set auth when env vars are absent."""
    with patch.dict(os.environ, {}, clear=True):
        dl = Http()
        assert dl.session.auth is None, (
            "Http should not set auth when httpfs_username/httpfs_password are unset"
        )


@patch.dict(os.environ, {"httpfs_username": "user", "httpfs_password": "pass"})
def test_http_sets_auth_from_env():
    """Verify that Http reads credentials from environment variables."""
    dl = Http()
    assert dl.session.auth == ("user", "pass"), (
        "Http should set session.auth from httpfs_username/httpfs_password env vars"
    )


def test_http_get_downloads_to_temp_file():
    """Verify that Http.get() downloads content to a temporary file."""
    dl = Http()
    content = b"archive-content-bytes"

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [content]
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(dl.session, "get", return_value=mock_response),
        dl.get("http://example.com/archive.tar.gz") as path,
    ):
        assert os.path.exists(path), "Downloaded file should exist"
        with open(path, "rb") as f:
            assert f.read() == content, "File content should match downloaded data"

    mock_response.raise_for_status.assert_called_once()


def test_http_get_raises_on_http_error():
    """Verify that Http.get() propagates HTTP errors."""
    dl = Http()

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("404 Not Found")

    with (
        patch.object(dl.session, "get", return_value=mock_response),
        pytest.raises(Exception, match="404"),
    ):
        dl.get("http://example.com/missing.tar.gz").__enter__()


def test_http_custom_chunk_size():
    """Verify that Http respects custom chunk_size."""
    dl = Http(chunk_size=4096)
    assert dl.chunk_size == 4096


def test_http_custom_tmp_dir(tmp_path):
    """Verify that Http uses custom tmp_dir for temp files."""
    dl = Http(tmp_dir=str(tmp_path))
    content = b"test-data"

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [content]
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(dl.session, "get", return_value=mock_response),
        dl.get("http://example.com/file.tar.gz") as path,
    ):
        assert path.startswith(str(tmp_path)), (
            "Temp file should be created in the specified tmp_dir"
        )


# ---------------------------------------------------------------------------
# S3Downloader tests
# ---------------------------------------------------------------------------


@patch("s3fs.S3FileSystem.open")
def test_s3_download_existing_file(mock_s3_open):
    """Verify that S3Downloader.get() downloads an existing S3 file."""
    content = b"s3-archive-content"
    mock_s3_open.return_value.__enter__ = MagicMock(return_value=BytesIO(content))
    mock_s3_open.return_value.__exit__ = MagicMock(return_value=False)

    dl = S3Downloader(anon=True)
    with dl.get("s3://bucket/key/archive.tar.gz") as path:
        assert os.path.exists(path), "Downloaded file should exist"
        with open(path, "rb") as f:
            assert f.read() == content, "File content should match S3 data"


@patch("s3fs.S3FileSystem.open")
def test_s3_download_nonexistent_file(mock_s3_open):
    """Verify that S3Downloader.get() raises on missing S3 file."""
    mock_s3_open.side_effect = FileNotFoundError("No such file")

    dl = S3Downloader(anon=True)
    with pytest.raises(FileNotFoundError), dl.get("s3://bucket/missing.tar.gz"):
        pass


@patch("s3fs.S3FileSystem.open")
def test_s3_custom_chunk_size(mock_s3_open):
    """Verify that S3Downloader respects custom chunk_size."""
    content = b"data"
    mock_s3_open.return_value.__enter__ = MagicMock(return_value=BytesIO(content))
    mock_s3_open.return_value.__exit__ = MagicMock(return_value=False)

    dl = S3Downloader(chunk_size=8192, anon=True)
    assert dl.chunk_size == 8192
