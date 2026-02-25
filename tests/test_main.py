from __future__ import annotations

from unittest.mock import patch

import pytest

from codaio_exporter.__main__ import error_exit


def test_error_exit_raises_system_exit() -> None:
    with pytest.raises(SystemExit) as exc_info:
        error_exit("test message")
    assert exc_info.value.code == 1


def test_error_exit_prints_message() -> None:
    with patch("builtins.print") as mock_print, pytest.raises(SystemExit):
        error_exit("please specify --api-token")
    mock_print.assert_called_once_with("please specify --api-token")
