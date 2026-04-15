"""
Tests for downloader implementations.

Downloaders provide a context manager ``get(src)`` that yields a local
file path for the consumer to process.  These tests cover the local
filesystem, HTTP, and S3 downloaders.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from insights_messaging.downloaders.httpfs import Http
from insights_messaging.downloaders.s3 import S3Downloader

# ---------------------------------------------------------------------------
# Http downloader tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("env_vars", "expected_auth"),
    [
        pytest.param({}, None, id="no_credentials"),
        pytest.param(
            {"httpfs_username": "user", "httpfs_password": "pass"},
            ("user", "pass"),
            id="with_credentials",
        ),
    ],
)
def test_http_auth_from_env(env_vars, expected_auth):
    """Http must configure session auth based on httpfs_username/httpfs_password env vars.

    When both env vars are present, session.auth should be a (username,
    password) tuple for Basic auth.  When absent, auth must be None —
    sending credentials to unauthenticated services would be incorrect.
    """
    with patch.dict(os.environ, env_vars, clear=True):
        dl = Http()
        assert dl.session.auth == expected_auth


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
