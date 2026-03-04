"""Tests for stdout initialization behavior in run_optimized_scan."""

import importlib
import sys


class _StdoutWithoutBuffer:
    """Simple stdout stand-in with no buffer attribute."""

    encoding = None

    def write(self, text):
        return len(text)

    def flush(self):
        return None


def test_run_optimized_scan_import_handles_none_stdout_encoding(monkeypatch):
    """Importing module should not crash when stdout.encoding is None."""
    monkeypatch.setattr(sys, 'stdout', _StdoutWithoutBuffer())
    sys.modules.pop('run_optimized_scan', None)

    module = importlib.import_module('run_optimized_scan')

    assert module is not None
