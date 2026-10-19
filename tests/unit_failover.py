# -*- coding: utf-8 -*-
"""ops-failover 单元断言：分类、窗口、决策、账本。

用法: python unit_failover.py [failover.py 路径]
覆盖：错误分类（状态码/措辞/本机错误）、路由键、窗口计数与预算、
      决策四态（observe/rotate/fallback/stop）、整链熔断、账本并发。
"""
import importlib.util
import json
import pathlib
import sys
import tempfile
import threading

_sys = sys
if hasattr(_sys.stdout, "reconfigure"):
    _sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = pathlib.Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent.parent
if not (ROOT / "failover.py").exists():
    ROOT = pathlib.Path.home() / "AppData/Local/hermes/plugins/ops-failover"
spec = importlib.util.spec_from_file_location("fo", ROOT / "failover.py")
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)

FAILS = []


def check(name, cond, extra=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f"  {extra}" if extra else ""))
    if not cond:
        FAILS.append(name)


def payload(**kw):
    base = {"provider": "commandcode", "model": "deepseek-v4.1-flash", "error_type": "APIStatusError",
            "status_code": 500, "error_message": "Internal Server Error"}
    base.update(kw)
    return base


def win(**over):
    return F.Window(F.resolve_config(over or None))


# ── 一、错误分类 ───────────────────────────────────────────────
check("500 → server_error", F.reason_from_payload(payload(status_code=500)) == "server_error")
check("502 → server_error", F.reason_from_payload(payload(status_code=502)) == "server_error")
check("503/529 → overloaded",
      F.reason_from_payload(payload(status_code=503)) == "overloaded"
      and F.reason_from_payload(payload(status_code=529)) == "overloaded")
check("408 → timeout", F.reason_from_payload(payload(status_code=408)) == "timeout")
check("429 → rate_limit", F.reason_from_payload(payload(status_code=429)) == "rate_limit")
check("400 → 不干预", F.reason_from_payload(payload(status_code=400, error_message="bad request")) is None)
check("字符串状态码也认", F.reason_from_payload(payload(status_code="503")) == "overloaded")
check("布尔状态码不当数字", F.reason_from_payload(payload(status_code=True, error_message="x")) is None)
check("auth 措辞压过 5xx",
      F.reason_from_payload(payload(status_code=500, error_message="Unauthorized: invalid api key")) is None)
check("TLS 失败不重路由",
      F.reason_from_payload(payload(status_code=500, error_message="SSL: CERTIFICATE_VERIFY_FAILED")) is None)
check("超时措辞 → timeout",
      F.reason_from_payload(payload(status_code=None, error_type="APIConnectionError",
                                    error_message="Connection reset by peer")) == "timeout")
check("非字典输入不炸", F.reason_from_payload(None) is None and F.reason_from_payload("x") is None)

# ── 二、本机错误 ───────────────────────────────────────────────
check("FileNotFoundError 被识别",
      F.local_error_kind({"error_type": "builtins.FileNotFoundError", "status_code": 500}) == "FileNotFoundError")
check("OSError / ConnectionError 不算本机错误",
      F.local_error_kind({"error_type": "OSError"}) is None
      and F.local_error_kind({"error_type": "ConnectionError"}) is None)
check("无类型无状态时看措辞",
      F.local_error_kind({"error_message": "[Errno 2] No such file or directory: 'x'"}) == "missing_path")
check("有状态码就不猜措辞",
      F.local_error_kind({"status_code": 500, "error_message": "No such file or directory"}) is None)

# ── 三、路由键 ────────────────────────────────────────────────
class _Req:
    url = "https://api.example.com/v1/chat"


class _Err:
    request = _Req()


check("路由键带 endpoint", F.route_key({**payload(), "error": _Err()}) == "commandcode|deepseek-v4.1-flash|api.example.com")
check("没有 endpoint 时退回 provider|model",
      F.route_key(payload()) == "commandcode|deepseek-v4.1-flash")

# ── 四、决策 ─────────────────────────────────────────────────
T = 1_000_000.0
w = win()
verdict, event = F.decide(w, payload(), now=T)
check("第 1 次只观察", verdict is None and event["action"] == "observe", event["action"])
F.decide(w, payload(), now=T + 10)
verdict, event = F.decide(w, payload(), now=T + 20)
check("第 3 次切兜底", verdict is not None and verdict["should_fallback"] is True and event["action"] == "fallback")
check("兜底不重试", verdict["retryable"] is False)

w2 = win()
F.decide(w2, payload(), now=T)
verdict, _ = F.decide(w2, payload(), now=T + 11 * 60)  # 窗口已过
check("窗口过期后重新计数", verdict is None)

w3 = win(stop_after=2)
F.decide(w3, payload(), now=T)
verdict, event = F.decide(w3, payload(), now=T + 5)
check("stop_after 达到即停手", verdict is not None and verdict["should_fallback"] is False
      and event["action"] == "stop")

w4 = win()
F.decide(w4, payload(status_code=429), now=T)
verdict, event = F.decide(w4, payload(status_code=429), now=T + 5)
check("限流第 2 次轮换凭据", verdict is not None and verdict.get("should_rotate_credential") is True
      and event["action"] == "rotate")
verdict, event = F.decide(w4, payload(status_code=429), now=T + 9)
check("限流累积到阈值改切兜底", event["action"] == "fallback", event["action"])

w5 = win()
verdict, event = F.decide(w5, {"provider": "p", "model": "m", "error_type": "FileNotFoundError",
                               "status_code": 500}, now=T)
check("本机错误只记录不干预", verdict is None and event["action"] == "decline"
      and event.get("local_error") == "FileNotFoundError")
check("本机错误不进窗口", w5.count(F.route_key({"provider": "p", "model": "m"}), T) == 0)

w6 = win()
verdict, event = F.decide(w6, payload(error_message="Unauthorized"), now=T)
check("认证失败不进窗口", verdict is None and w6.count(F.route_key(payload()), T) == 0)

w7 = win(enabled=False)
verdict, event = F.decide(w7, payload(), now=T)
check("关闭时不干预", verdict is None and event["action"] == "disabled")

w8 = win(max_actions_per_hour=1)
F.decide(w8, payload(), now=T)
F.decide(w8, payload(), now=T + 1)
F.decide(w8, payload(), now=T + 2)          # 第 3 次 → fallback，用掉唯一预算
before = w8.count(F.route_key(payload()), T + 3)
verdict, event = F.decide(w8, payload(), now=T + 3)
check("预算用完就不再干预", verdict is None and event["action"] == "budget_exhausted")
check("预算用完不改窗口状态", w8.count(F.route_key(payload()), T + 3) == before)

w9 = win(chain_stop_after=3, fallback_after=1)
for index in range(3):
    F.decide(w9, {"provider": f"p{index}", "model": "m", "status_code": 500}, now=T + index)
verdict, event = F.decide(w9, {"provider": "p9", "model": "m", "status_code": 500}, now=T + 4)
check("短窗口内多条路由降级 → 整链熔断", event["action"] == "chain_stop"
      and verdict is not None and verdict["should_fallback"] is False, event["action"])

# ── 五、账本并发 ─────────────────────────────────────────────
tmp = pathlib.Path(tempfile.mkdtemp())
ledger = tmp / "failover.jsonl"
THREADS, PER = 8, 40


def writer():
    for i in range(PER):
        F.append_ledger(ledger, {"ts": 0, "action": "observe", "route": "p|m", "seq": i})


threads = [threading.Thread(target=writer) for _ in range(THREADS)]
[t.start() for t in threads]
[t.join() for t in threads]
lines = [l for l in ledger.read_text(encoding="utf-8").splitlines() if l.strip()]
broken = 0
for line in lines:
    try:
        json.loads(line)
    except Exception:
        broken += 1
check("账本并发不丢行", len(lines) == THREADS * PER, f"{len(lines)}/{THREADS * PER}")
check("账本并发不写坏行", broken == 0, f"{broken} 行坏")

print()
if FAILS:
    print(f"=== {len(FAILS)} 项失败：" + "；".join(FAILS))
    sys.exit(1)
print("=== 全部通过")
