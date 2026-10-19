"""ops-failover: escalate a dead route instead of paying the retry ramp.

Registered on ``transform_api_error_classification`` — the hook Hermes consults BEFORE the
built-in pipeline, once per failed API call. Returning a verdict replaces the built-in one,
so this plugin declines (returns None) unless a route has already proven itself bad inside a
rolling window; see ``failover.decide``.

Why a window is needed at all: Hermes' own rules are per-call. A transport failure falls back
after ``retry_count >= 2``, and at max retries it first rebuilds the *same* primary client and
resets the counter (``_try_recover_primary_transport``). That is fine for one bad minute, and it
is exactly how a genuinely dead route produces minutes-long hangs and a long tail of repeated
attempts — each of which re-sends the whole context. A rolling per-route count turns "the fourth
failure this window" into "switch now".

Observer discipline: never raises, never changes arguments, never overrides a reason it did not
name itself. Everything local; no network, no credentials.

Config (``config.yaml`` -> ``plugins.entries.ops-failover``):
  enabled               true   master switch
  window_minutes        10     rolling window per route
  rotate_after          2      429s in the window before asking for a credential rotation
  fallback_after        3      transport failures in the window before skipping the retry ramp
  stop_after            0      >=N failures -> end the turn instead of cascading (0 = never)
  max_actions_per_hour  20     circuit breaker: stop intervening past this many actions/hour
  max_tracked_routes    50     bound the in-memory route table
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

from . import failover

logger = logging.getLogger(__name__)

PLUGIN_NAME = "ops-failover"

_HELP = """\
/failover: what ops-failover saw and did

  /failover             last 24 hours + the current window counts
  /failover 2h          any number of hours or days (6h, 7d, all)
  /failover status      only the live per-route window counts
  /failover clear       clear the ledger and the in-memory window
"""

_window: Optional[failover.Window] = None
_config_cache: tuple[float, dict[str, Any]] = (0.0, {})
_CONFIG_TTL = 5.0


def _data_dir() -> Path:
    from plugins.plugin_storage import plugin_data_dir

    return plugin_data_dir(PLUGIN_NAME)


def _ledger_path() -> Path:
    return _data_dir() / failover.LEDGER_FILENAME


def _config() -> dict[str, Any]:
    global _config_cache
    now = time.monotonic()
    cached_at, cached = _config_cache
    if cached and now - cached_at < _CONFIG_TTL:
        return cached
    raw: Any = None
    try:
        from hermes_cli.config import load_config_readonly

        config = load_config_readonly()
        plugins = config.get("plugins") if isinstance(config, dict) else None
        entries = plugins.get("entries") if isinstance(plugins, dict) else None
        raw = entries.get(PLUGIN_NAME) if isinstance(entries, dict) else None
    except Exception as exc:
        logger.debug("ops-failover: config unavailable (%s), using defaults", exc)
    cfg = failover.resolve_config(raw)
    _config_cache = (now, cfg)
    return cfg


def _state() -> failover.Window:
    global _window
    cfg = _config()
    if _window is None:
        _window = failover.Window(cfg)
    else:
        _window.cfg = cfg
    return _window


def evaluate(payload: dict[str, Any], *, now: Optional[float] = None) -> Optional[dict[str, Any]]:
    """Decision + ledger write for one failed API call. Returns the verdict or None."""
    stamp = time.time() if now is None else now
    window = _state()
    verdict, event = failover.decide(window, payload, now=stamp)
    try:
        failover.append_ledger(_ledger_path(), event)
    except Exception as exc:
        logger.debug("ops-failover: could not append ledger: %s", exc)
    return verdict


def _on_classify(**payload: Any) -> Optional[dict[str, Any]]:
    try:
        return evaluate(payload)
    except Exception as exc:  # fail-open: a broken policy must not affect error handling
        logger.debug("ops-failover: decision failed, declining: %s", exc)
        return None


def _handle_failover(raw_args: str = "") -> Optional[str]:
    words = (raw_args or "").lower().split()
    known = {"status", "clear", "all", "help", "-h", "--help"}
    if "help" in words or "-h" in words or "--help" in words:
        return _HELP
    unknown = [w for w in words if w not in known and not _span(w)]
    if unknown:
        return f"Unknown option: {unknown[0]}\n\n{_HELP}"
    path = _ledger_path()
    if "clear" in words:
        global _window
        _window = None
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            return f"ops-failover: 清空失败 {exc}"
        return "ops-failover: 账本与窗口计数都已清空（插件仍启用）。"
    now = time.time()
    window = _state()
    if "status" in words:
        rows = failover.summarize([], title="仅窗口计数", now=now, window=window).splitlines()
        return "\n".join(rows[1:]) if len(rows) > 1 else "\n".join(rows)
    span, title = 24 * 3600.0, "近 24 小时"
    if "all" in words:
        span, title = 0.0, "全部"
    else:
        for word in words:
            parsed = _span(word)
            if parsed:
                span, title = parsed
                break
    rows = failover.read_ledger(path, since=None if span == 0 else now - span)
    return failover.summarize(rows, title=title, now=now, window=window)


def _span(word: str) -> Optional[tuple[float, str]]:
    if len(word) < 2 or word[-1] not in "hd":
        return None
    try:
        amount = float(word[:-1])
    except ValueError:
        return None
    if amount <= 0:
        return None
    return (amount * 3600.0, f"近 {amount:g} 小时") if word[-1] == "h" else (amount * 86400.0, f"近 {amount:g} 天")


def register(ctx) -> None:
    ctx.register_hook("transform_api_error_classification", _on_classify)
    ctx.register_command(
        "failover",
        handler=_handle_failover,
        args_hint="[2h|7d|all|status] | clear",
        description="Show how ops-failover escalated repeated API failures per route.",
    )
