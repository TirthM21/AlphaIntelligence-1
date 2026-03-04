import os

import pytest

from src.config.settings import SettingsValidationError, load_settings


def test_load_settings_success():
    settings = load_settings('config.yaml')
    assert settings.fetcher.max_retries >= 1
    assert settings.ai_agent.max_tokens > 0


def test_load_settings_env_override(monkeypatch):
    monkeypatch.setenv('ALPHA_FETCHER_MAX_RETRIES', '7')
    settings = load_settings('config.yaml')
    assert settings.fetcher.max_retries == 7


def test_load_settings_missing_runtime(tmp_path):
    bad = tmp_path / 'bad.yaml'
    bad.write_text('stock_universe: []\n', encoding='utf-8')

    with pytest.raises(SettingsValidationError, match="missing key 'runtime'"):
        load_settings(str(bad))
