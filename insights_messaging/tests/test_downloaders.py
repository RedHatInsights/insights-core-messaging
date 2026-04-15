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
    """LocalFS.get() must yield the canonical (realpath) of the source file.

    The consumer passes the yielded path to the engine for extraction.
    Using realpath ensures consistent behavior regardless of how the
    path was originally specified (relative, with .., etc.).
    """
    fs = LocalFS()
    with tempfile.NamedTemporaryFile() as f, fs.get(f.name) as path:
        assert path == os.path.realpath(f.name), (
            f"LocalFS.get() should yield the realpath of the source, got {path!r}"
        )


def test_localfs_resolves_symlinks(tmp_path):
    """LocalFS.get() must resolve symbolic links to the real file.

    In production, archive paths may be symlinks (e.g. from shared
    storage mounts).  The engine expects the real filesystem path so
    that extraction and cleanup work correctly.
    """
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
    """LocalFS.get() must expand ~ (tilde) to the user's home directory.

    Configuration files may use ~ as shorthand.  Without expansion,
    the engine would look for a literal '~' directory and fail.
    """
    fs = LocalFS()
    with fs.get("~") as path:
        assert path == os.path.realpath(os.path.expanduser("~")), (
            "LocalFS.get() should expand ~ to the home directory"
        )


# ---------------------------------------------------------------------------
# Http downloader tests
# ---------------------------------------------------------------------------


@patch.dict(os.environ, {}, clear=True)
def test_http_no_auth_by_default():
    """Http must not set session auth when credential env vars are absent.

    In environments without authentication (e.g. internal services behind
    a VPN), sending credentials would be incorrect.  The Http downloader
    reads httpfs_username/httpfs_password from the environment and must
    leave session.auth as None when they are not set.
    """
    with patch.dict(os.environ, {}, clear=True):
        dl = Http()
        assert dl.session.auth is None, (
            "Http should not set auth when httpfs_username/httpfs_password are unset"
        )


@patch.dict(os.environ, {"httpfs_username": "user", "httpfs_password": "pass"})
def test_http_sets_auth_from_env():
    """Http must configure session auth from httpfs_username/httpfs_password env vars.

    When both env vars are present, session.auth should be set as a
    (username, password) tuple so that every HTTP request includes
    Basic auth headers.
    """
    dl = Http()
    assert dl.session.auth == ("user", "pass"), (
        "Http should set session.auth from httpfs_username/httpfs_password env vars"
    )


def test_http_get_downloads_to_temp_file():
    """Http.get() must stream the response body into a temporary file.

    The consumer needs a local file path for the engine to extract.
    Http.get() downloads the archive via chunked streaming, writes it
    to a temp file, and yields the path.  This test verifies the file
    exists and contains the expected bytes, and that raise_for_status()
    is called to catch HTTP errors early.
    """
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
    """Http.get() must propagate HTTP errors (4xx/5xx) to the caller.

    The consumer relies on exceptions to trigger error handling and
    watcher notifications.  Silently swallowing HTTP errors would cause
    the engine to process an empty or corrupt file.
    """
    dl = Http()

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("404 Not Found")

    with (
        patch.object(dl.session, "get", return_value=mock_response),
        pytest.raises(Exception, match="404"),
    ):
        dl.get("http://example.com/missing.tar.gz").__enter__()


def test_http_custom_tmp_dir(tmp_path):
    """Http must create temp files in the configured tmp_dir.

    In production, the default /tmp may be too small or on a different
    filesystem.  The tmp_dir parameter allows operators to point
    downloads at a volume with sufficient space.
    """
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
    """S3Downloader.get() must download an S3 object to a local temp file.

    The downloader opens the S3 object as a stream, copies it to a
    NamedTemporaryFile using shutil.copyfileobj, and yields the local
    path.  This test verifies the file is created and its content
    matches the S3 object body.
    """
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
    """S3Downloader.get() must raise FileNotFoundError for missing S3 keys.

    When the archive has been deleted or the S3 key is wrong, the error
    must propagate so the consumer can log it and notify watchers rather
    than silently processing nothing.
    """
    mock_s3_open.side_effect = FileNotFoundError("No such file")

    dl = S3Downloader(anon=True)
    with pytest.raises(FileNotFoundError), dl.get("s3://bucket/missing.tar.gz"):
        pass


@patch("s3fs.S3FileSystem.open")
def test_s3_custom_chunk_size(mock_s3_open):
    """S3Downloader must work correctly with a custom chunk_size.

    Large archives benefit from bigger chunks to reduce syscall overhead.
    This test verifies that a non-default chunk_size does not break the
    download-to-temp-file flow.
    """
    content = b"data"
    mock_s3_open.return_value.__enter__ = MagicMock(return_value=BytesIO(content))
    mock_s3_open.return_value.__exit__ = MagicMock(return_value=False)

    dl = S3Downloader(chunk_size=8192, anon=True)
    with dl.get("s3://bucket/key/archive.tar.gz") as path:
        assert os.path.exists(path), "Downloaded file should exist"
