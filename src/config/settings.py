"""Application settings loader with YAML + environment overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Dict, Optional, Type, TypeVar, get_args, get_origin, get_type_hints

import yaml


class SettingsValidationError(ValueError):
    """Raised when configuration is invalid."""


@dataclass(frozen=True)
class FetcherSettings:
    cache_dir: str = "./data/cache"
    cache_expiry_hours: int = 24
    max_retries: int = 3
    retry_delay_seconds: int = 2


@dataclass(frozen=True)
class AIAgentSettings:
    base_url: str = "https://integrate.api.nvidia.com/v1"
    default_model: str = "moonshotai/kimi-k2.5"
    request_timeout_seconds: float = 45.0
    max_retries: int = 1
    max_tokens: int = 4096
    failure_threshold: int = 2
    cooldown_seconds: int = 900


@dataclass(frozen=True)
class OptimizedBatchProcessorSettings:
    results_dir: str = "./data/batch_results"
    max_workers: int = 5
    rate_limit_delay: float = 0.2
    batch_size: int = 100
    prefetch_batch_size: int = 500
    prefetch_pause_seconds: float = 2.0
    rate_limit_cooldown_seconds: int = 30
    rate_limit_error_threshold: int = 3
    max_backoff_delay: float = 5.0


@dataclass(frozen=True)
class AppSettings:
    fetcher: FetcherSettings
    ai_agent: AIAgentSettings
    optimized_batch_processor: OptimizedBatchProcessorSettings


T = TypeVar("T")


def _coerce(value: Any, target_type: Type[Any]) -> Any:
    origin = get_origin(target_type)
    if origin is Optional:
        arg = get_args(target_type)[0]
        return None if value is None else _coerce(value, arg)

    if target_type is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        raise ValueError(f"expected bool, got {value!r}")
    if target_type is int:
        return int(value)
    if target_type is float:
        return float(value)
    if target_type is str:
        return str(value)
    return value


def _merge_env_overrides(section_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(data)
    for key in list(merged.keys()):
        env_key = f"ALPHA_{section_name.upper()}_{key.upper()}"
        if env_key in os.environ:
            merged[key] = os.environ[env_key]
    return merged


def _build_dataclass(section_name: str, cls: Type[T], raw: Dict[str, Any]) -> T:
    raw = _merge_env_overrides(section_name, raw)
    values: Dict[str, Any] = {}
    errors = []
    type_hints = get_type_hints(cls)

    for field in fields(cls):
        if field.name not in raw:
            errors.append(f"missing key '{section_name}.{field.name}'")
            continue
        try:
            values[field.name] = _coerce(raw[field.name], type_hints.get(field.name, field.type))
        except (TypeError, ValueError) as exc:
            errors.append(
                f"invalid '{section_name}.{field.name}'={raw[field.name]!r}: {exc}"
            )

    if errors:
        raise SettingsValidationError("\n".join(errors))
    return cls(**values)


def _validate_semantics(settings: AppSettings) -> None:
    errors = []

    if settings.fetcher.cache_expiry_hours <= 0:
        errors.append("fetcher.cache_expiry_hours must be > 0")
    if settings.fetcher.max_retries < 1:
        errors.append("fetcher.max_retries must be >= 1")
    if settings.fetcher.retry_delay_seconds < 0:
        errors.append("fetcher.retry_delay_seconds must be >= 0")

    if settings.ai_agent.request_timeout_seconds <= 0:
        errors.append("ai_agent.request_timeout_seconds must be > 0")
    if settings.ai_agent.max_retries < 0:
        errors.append("ai_agent.max_retries must be >= 0")
    if settings.ai_agent.max_tokens <= 0:
        errors.append("ai_agent.max_tokens must be > 0")
    if settings.ai_agent.failure_threshold < 1:
        errors.append("ai_agent.failure_threshold must be >= 1")
    if settings.ai_agent.cooldown_seconds < 0:
        errors.append("ai_agent.cooldown_seconds must be >= 0")

    bp = settings.optimized_batch_processor
    if bp.max_workers < 1:
        errors.append("optimized_batch_processor.max_workers must be >= 1")
    if bp.rate_limit_delay <= 0:
        errors.append("optimized_batch_processor.rate_limit_delay must be > 0")
    if bp.batch_size < 1:
        errors.append("optimized_batch_processor.batch_size must be >= 1")
    if bp.prefetch_batch_size < 1:
        errors.append("optimized_batch_processor.prefetch_batch_size must be >= 1")
    if bp.prefetch_pause_seconds < 0:
        errors.append("optimized_batch_processor.prefetch_pause_seconds must be >= 0")
    if bp.rate_limit_cooldown_seconds < 0:
        errors.append("optimized_batch_processor.rate_limit_cooldown_seconds must be >= 0")
    if bp.rate_limit_error_threshold < 1:
        errors.append("optimized_batch_processor.rate_limit_error_threshold must be >= 1")
    if bp.max_backoff_delay < 0:
        errors.append("optimized_batch_processor.max_backoff_delay must be >= 0")

    if errors:
        raise SettingsValidationError("\n".join(errors))


_SETTINGS_CACHE: Optional[AppSettings] = None


def load_settings(config_path: str = "config.yaml") -> AppSettings:
    """Load strongly typed app settings from YAML with env overrides."""
    path = Path(config_path)
    if not path.exists():
        raise SettingsValidationError(f"Config file not found: {path}")

    try:
        raw_config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise SettingsValidationError(f"Failed to parse config file {path}: {exc}") from exc

    runtime = raw_config.get("runtime")
    if not isinstance(runtime, dict):
        raise SettingsValidationError("missing key 'runtime' in config.yaml")

    try:
        settings = AppSettings(
            fetcher=_build_dataclass("fetcher", FetcherSettings, runtime.get("fetcher") or {}),
            ai_agent=_build_dataclass("ai_agent", AIAgentSettings, runtime.get("ai_agent") or {}),
            optimized_batch_processor=_build_dataclass(
                "optimized_batch_processor",
                OptimizedBatchProcessorSettings,
                runtime.get("optimized_batch_processor") or {},
            ),
        )
    except SettingsValidationError as exc:
        raise SettingsValidationError(f"Settings validation failed:\n{exc}") from exc

    _validate_semantics(settings)
    return settings


def get_settings(config_path: str = "config.yaml") -> AppSettings:
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is None:
        _SETTINGS_CACHE = load_settings(config_path=config_path)
    return _SETTINGS_CACHE
