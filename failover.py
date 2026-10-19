"""Pure decision logic for ops-failover.

No Hermes imports: the hook is a thin adapter, this module owns the policy and is
directly unit-testable with an injected clock.

Policy in one sentence: a single transport failure is noise, but the same route
(provider + model) failing repeatedly inside a short window is a dead route — stop
paying the retry ramp (which can be minutes per attempt) and move to the fallback
chain on the next failure instead of the fourth.
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "window_minutes": 10,
    "rotate_after": 2,
    "fallback_after": 3,
    "stop_after": 0,
    "max_actions_per_hour": 20,
    "max_tracked_routes": 50,
    "chain_stop_after": 3,        # 0 = 关闭；短窗口内这么多条路由被降级 → 整链熔断
    "chain_window_seconds": 90,
}

LEDGER_FILENAME = "failover.jsonl"

# Wire-level classes this plugin is willing to name. Anything else -> decline, so the
# built-in classifier keeps full authority (auth, billing, content policy, TLS, format:
# all have their own semantics and must not be re-routed by a counting rule).
TRANSPORT_REASONS = frozenset({"timeout", "server_error", "overloaded", "unknown"})
POOL_REASONS = frozenset({"rate_limit", "upstream_rate_limit"})
WATCHED_REASONS = TRANSPORT_REASONS | POOL_REASONS

# Failures that are local to THIS machine: switching providers re-sends the same bytes through
# the same broken local path, so they are never re-routed. OSError/ConnectionError are
# deliberately absent — they are the base classes of real transport failures, and calling those
# "local" would stop failover exactly when it matters.
LOCAL_ERROR_TYPES = ("FileNotFoundError", "PermissionError", "IsADirectoryError", "NotADirectoryError",
                     "ModuleNotFoundError", "ImportError", "UnicodeDecodeError", "UnicodeEncodeError")


def local_error_kind(payload: Any) -> Optional[str]:
    """Name a path/permission/import failure, or None.

    Recorded (never re-routed) because an environment swap or a deleted file is indistinguishable
    from a provider outage in the raw logs: 2026-10-19, a stale PM environment raised
    ``FileNotFoundError`` inside TLS setup and every session looked like "provider not responding".
    """
    if not isinstance(payload, dict):
        return None
    etype = str(payload.get("error_type") or "")
    short = etype.rsplit(".", 1)[-1]
    if short in LOCAL_ERROR_TYPES:
        return short
    # Message-only signal, kept narrow: no error_type AND no HTTP status at all.
    if not etype and payload.get("status_code") in (None, ""):
        message = str(payload.get("error_message") or "")
        if "No such file or directory" in message or "Errno 2" in message:
            return "missing_path"
    return None


_TIMEOUT_TOKENS = ("timeout", "timed out", "connectionerror", "apiconnection", "connection reset",
                   "connection aborted", "readtimeout", "connecttimeout", "remotedisconnected",
                   "eof occurred", "broken pipe")
_TLS_TOKENS = ("ssl", "certificate", "cert verify", "tls")
# Non-transport failures that can still carry transport-ish wording, or arrive wrapped in a
# 5xx by a gateway. Declining these keeps a real auth/policy/billing rejection from being
# counted as an outage and masked by a route switch (independent review, 2026-10-16).
_NON_TRANSPORT_TOKENS = ("unauthorized", "forbidden", "auth", "api key", "apikey", "invalid_request",
                         "malformed", "content policy", "content_policy", "safety", "blocked by",
                         "insufficient balance", "insufficient_quota", "billing", "entitlement",
                         "not entitled", "model not found", "does not exist", "permission")


def resolve_config(raw: Any) -> dict[str, Any]:
    """Merge a config mapping over DEFAULTS, ignoring unknown keys and bad types."""
    cfg = dict(DEFAULTS)
    if isinstance(raw, dict):
        for key, default in DEFAULTS.items():
            value = raw.get(key)
            if isinstance(default, bool):
                if isinstance(value, bool):
                    cfg[key] = value
            elif isinstance(default, int):
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    cfg[key] = value
    return cfg


def reason_from_payload(payload: Any) -> Optional[str]:
    """Name the failure in FailoverReason terms, or None to stay out of the way."""
    if not isinstance(payload, dict):
        return None
    status = payload.get("status_code")
    error_type = str(payload.get("error_type") or "")
    message = str(payload.get("error_message") or "")
    haystack = f"{error_type} {message}".lower()
    if any(token in haystack for token in _TLS_TOKENS):
        return None  # deterministic chain failure: the built-in says fail fast, don't re-route
    if any(token in haystack for token in _NON_TRANSPORT_TOKENS):
        return None  # auth/policy/billing wording outranks transport wording, whatever the status
    if isinstance(status, bool):  # True == 1 would read as a status code below
        status = None
    elif isinstance(status, str) and status.strip().isdigit():
        status = int(status.strip())  # some SDKs stringify the code
    if isinstance(status, int):
        if status == 408:
            return "timeout"  # server-side timeout: retryable in the same family as client timeouts
        if status == 429:
            return "rate_limit"
        if status in (500, 502):
            return "server_error"
        if status in (503, 529):
            return "overloaded"
        if status >= 500:
            return "server_error"
        return None  # an explicit 1xx/2xx/3xx/4xx is not a transport failure, whatever the text says
    if any(token in haystack for token in _TIMEOUT_TOKENS):
        return "timeout"
    if not status and error_type and error_type.lower() in {"apiconnectionerror", "apitimeouterror"}:
        return "timeout"
    return None


def host_from_payload(payload: Any) -> str:
    """Best-effort endpoint host for this failure, so a dead base_url and a healthy
    fallback on the same provider+model are counted as the different routes they are.

    The hook payload carries no base_url, so read the request off the exception the
    provider client raised; "" when the SDK attached nothing (route falls back to
    provider|model, which only merges same-endpoint failures).
    """
    if not isinstance(payload, dict):
        return ""
    try:
        error = payload.get("error")
        request = getattr(error, "request", None)
        url = getattr(request, "url", None)
        if url:
            from urllib.parse import urlsplit

            return urlsplit(str(url)).netloc.lower()
    except Exception:
        pass
    body = payload.get("error_body")
    if isinstance(body, dict):
        for key in ("base_url", "url", "endpoint"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value.split("//")[-1].split("/")[0].lower()
    return ""


def route_key(payload: Any) -> str:
    if not isinstance(payload, dict):
        payload = {}
    provider = str(payload.get("provider") or "?")
    model = str((payload or {}).get("model") or "?")
    host = host_from_payload(payload)
    return f"{provider}|{model}|{host}" if host else f"{provider}|{model}"


class Window:
    """Rolling failure counts per route, plus an hourly action budget."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.failures: dict[str, list[float]] = {}
        self.actions: list[float] = []
        self.escalations: list[tuple[float, str]] = []

    def note_failure(self, key: str, now: float) -> int:
        """Record a failure and return how many are inside the window (including this one)."""
        window = float(self.cfg["window_minutes"]) * 60.0
        stamps = [t for t in self.failures.get(key, []) if now - t <= window]
        stamps.append(now)
        self.failures[key] = stamps
        self._evict(now)
        return len(stamps)

    def count(self, key: str, now: float) -> int:
        window = float(self.cfg["window_minutes"]) * 60.0
        return len([t for t in self.failures.get(key, []) if now - t <= window])

    def budget_left(self, now: float) -> bool:
        horizon = 3600.0
        self.actions = [t for t in self.actions if now - t <= horizon]
        return len(self.actions) < int(self.cfg["max_actions_per_hour"])

    def note_action(self, now: float) -> None:
        self.actions.append(now)

    def note_escalation(self, key: str, now: float) -> None:
        """Record that one route was escalated (fallback/stop): feeds the whole-chain breaker."""
        self.escalations.append((now, key))
        if len(self.escalations) > 200:  # bounded memory; the burst window is seconds, not days
            self.escalations = self.escalations[-200:]

    def recent_escalations(self, now: float) -> int:
        span = float(self.cfg["chain_window_seconds"])
        return len([t for t, _ in self.escalations if now - t <= span])

    def _evict(self, now: float) -> None:
        limit = int(self.cfg["max_tracked_routes"])
        if len(self.failures) <= limit:
            return
        for key in sorted(self.failures, key=lambda k: self.failures[k][-1])[: len(self.failures) - limit]:
            self.failures.pop(key, None)

    def snapshot(self, now: float) -> list[tuple[str, int, float]]:
        rows = [(key, self.count(key, now), self.failures[key][-1] if self.failures.get(key) else 0.0)
                for key in self.failures]
        return sorted(rows, key=lambda row: row[2], reverse=True)


def decide(window: Window, payload: Any, *, now: float) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    """Return (verdict_for_hermes, ledger_event).

    ``verdict`` is None whenever this plugin declines, which is the common case: the
    built-in classifier keeps full authority until a route has proven itself bad.
    """
    if not isinstance(payload, dict):
        payload = {}
    cfg = window.cfg
    key = route_key(payload)
    reason = reason_from_payload(payload)
    base_event: dict[str, Any] = {
        "ts": now, "route": key, "reason": reason,
        "error_type": str((payload or {}).get("error_type") or ""),
        "status_code": (payload or {}).get("status_code"),
    }
    if not cfg["enabled"]:
        return None, {**base_event, "action": "disabled"}
    local = local_error_kind(payload)
    if local:
        # Reined in even when a status code made it look like an outage: the request never left
        # this machine, so counting it as a bad route would punish a healthy provider.
        return None, {**base_event, "reason": None, "local_error": local, "action": "decline"}
    if reason is None or reason not in WATCHED_REASONS:
        return None, {**base_event, "action": "decline"}

    if not window.budget_left(now):
        return None, {**base_event, "action": "budget_exhausted"}  # no-op: do not mutate route state
    count = window.note_failure(key, now)
    base_event["window_count"] = count

    # Whole-chain breaker: several routes already escalated inside a short burst means the
    # fallback chain itself is dead; cycling the remaining routes only burns wall-clock and
    # retries, so stop cleanly instead (A/B, 2026-10-18: 28s -> ~10s with every route dead).
    if int(cfg["chain_stop_after"]) and window.recent_escalations(now) >= int(cfg["chain_stop_after"]):
        window.note_action(now)
        return (
            {"reason": reason, "retryable": False, "should_fallback": False,
             "message": (f"{cfg['chain_window_seconds']} 秒内已有 {window.recent_escalations(now)} 条路由被降级，"
                         f"整条通道链看起来都是坏的，直接停手（chain_stop_after={cfg['chain_stop_after']}）。"),
             "error_context": {"ops_failover": {"route": key, "window_count": count, "action": "chain_stop"}}},
            {**base_event, "action": "chain_stop"},
        )

    if reason in POOL_REASONS and count < int(cfg["fallback_after"]):
        # Rotate credentials first; past fallback_after failures even a pool reason escalates
        # to the fallback chain instead of rotating forever (independent review, 2026-10-16).
        if count < int(cfg["rotate_after"]):
            return None, {**base_event, "action": "observe"}
        window.note_action(now)
        return (
            {"reason": reason, "should_rotate_credential": True,
             "message": f"{key} 在 {cfg['window_minutes']} 分钟内第 {count} 次限流，已请求轮换凭据。",
             "error_context": {"ops_failover": {"route": key, "window_count": count, "action": "rotate"}}},
            {**base_event, "action": "rotate"},
        )

    # Transport family.
    if int(cfg["stop_after"]) and count >= int(cfg["stop_after"]):
        window.note_action(now)
        window.note_escalation(key, now)
        return (
            {"reason": reason, "retryable": False, "should_fallback": False,
             "message": (f"{key} 在 {cfg['window_minutes']} 分钟内第 {count} 次传输失败，"
                         f"已达到 ops-failover 的止损阈值（stop_after={cfg['stop_after']}）。"),
             "error_context": {"ops_failover": {"route": key, "window_count": count, "action": "stop"}}},
            {**base_event, "action": "stop"},
        )
    if count < int(cfg["fallback_after"]):
        return None, {**base_event, "action": "observe"}
    window.note_action(now)
    window.note_escalation(key, now)
    return (
        {"reason": reason, "retryable": False, "should_fallback": True,
         "message": (f"{key} 在 {cfg['window_minutes']} 分钟内第 {count} 次 {reason} 失败，"
                     f"跳过重试直接切兜底通道。"),
         "error_context": {"ops_failover": {"route": key, "window_count": count, "action": "fallback"}}},
        {**base_event, "action": "fallback"},
    )


_LEDGER_LOCK = threading.Lock()
_LOCK_WAIT_S = 5.0


@contextmanager
def _process_lock(lock_path: Path):
    """Exclusive lock across processes; best effort — a lock failure never loses the row."""
    handle = None
    locked = False
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(lock_path, "a+b")
        if os.name == "nt":
            import msvcrt

            deadline = time.monotonic() + _LOCK_WAIT_S
            while True:
                try:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    locked = True
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.005)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            locked = True
    except Exception:
        locked = False
    try:
        yield locked
    finally:
        try:
            if locked and handle is not None:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            if handle is not None:
                handle.close()
        except Exception:
            pass


def append_ledger(path: Path, row: dict[str, Any]) -> None:
    """Append one JSON line without losing it.

    Text-mode ``open("a")`` writes the line in pieces on Windows, so concurrent writers interleave
    and lines disappear (measured: 320 threaded appends produced 301). One ``os.write`` of the
    pre-encoded bytes lands as a unit; threads are serialized in-process and other processes by the
    ``.lock`` file beside the ledger.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8", "surrogatepass")
    with _LEDGER_LOCK, _process_lock(path.with_name(path.name + ".lock")):
        for attempt in (0, 1):
            try:
                handle = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0))
                try:
                    os.write(handle, blob)
                finally:
                    os.close(handle)
                return
            except OSError:
                if attempt:
                    raise


def read_ledger(path: Path, since: float | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if since is not None and float(row.get("ts") or 0) < since:
                    continue
                rows.append(row)
    except OSError:
        return rows
    return rows


def summarize(rows: Iterable[dict[str, Any]], *, title: str, now: float | None = None,
              window: Optional[Window] = None) -> str:
    rows = list(rows)
    now = time.time() if now is None else now
    out = [
        f"ops-failover · {title}",
        "  作用：同一 provider+model 在一个窗口内反复传输失败时，跳过重试直接切兜底通道；"
        "限流则请求轮换凭据。其余情况一律不干预（返回 None，交回内建分类器）。",
    ]
    if not rows:
        out.append("  这个窗口内没有记录。")
    else:
        by_action: dict[str, int] = {}
        for row in rows:
            action = str(row.get("action") or "?")
            by_action[action] = by_action.get(action, 0) + 1
        order = ["fallback", "rotate", "stop", "observe", "decline", "budget_exhausted", "disabled"]
        parts = [f"{a}={by_action[a]}" for a in order if a in by_action]
        parts += [f"{a}={n}" for a, n in by_action.items() if a not in order]
        out.append(f"  事件 {len(rows)} 条：" + "，".join(parts))
        local_rows = [r for r in rows if r.get("local_error")]
        if local_rows:
            counts: dict[str, int] = {}
            for row in local_rows:
                name = str(row.get("local_error"))
                counts[name] = counts.get(name, 0) + 1
            detail = "、".join(f"{k}×{v}" for k, v in sorted(counts.items(), key=lambda kv: -kv[1])[:3])
            out.append(f"  本地/环境类错误 {len(local_rows)} 次（{detail}）——**不是链路问题**："
                       f"先查路径/环境是否被换掉（升级换代后必须重启后端），别按通道故障排查")
        acted = [r for r in rows if r.get("action") in {"fallback", "rotate", "stop"}]
        if acted:
            out.append("  最近几次干预：")
            for row in acted[-6:]:
                stamp = time.strftime("%m-%d %H:%M:%S", time.localtime(float(row.get("ts") or now)))
                out.append(f"    {stamp}  {row.get('action'):8s} {row.get('route')}  "
                           f"{row.get('reason')} (窗口内第 {row.get('window_count')} 次)")
    if window is not None:
        live = [row for row in window.snapshot(now) if row[1] > 0]
        if live:
            out.append("  当前窗口内的失败计数：")
            for key, count, last in live[:8]:
                stamp = time.strftime("%H:%M:%S", time.localtime(last))
                out.append(f"    {key:44s} {count} 次，最近 {stamp}")
    return "\n".join(out)
