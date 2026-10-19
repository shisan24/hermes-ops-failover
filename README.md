# hermes-ops-failover

[![ci](https://github.com/shisan24/hermes-ops-failover/actions/workflows/ci.yml/badge.svg)](https://github.com/shisan24/hermes-ops-failover/actions/workflows/ci.yml)

同一路由（provider + model + endpoint）在一个窗口内反复传输失败时，**跳过重试阶梯直接切兜底通道**；
限流则先请求轮换凭据。其余情况一概不干预，把判断交回 Hermes 内建的分类器。

## 安装

```bash
git clone https://github.com/shisan24/hermes-ops-failover ~/.hermes/plugins/ops-failover
hermes plugins enable ops-failover
```

## 它做什么

| 失败类型 | 窗口内达到 | 动作 |
|---|---|---|
| `timeout` / `server_error` / `overloaded`（可命名的传输类） | `fallback_after`（默认 3） | `retryable=False` + `should_fallback=True`：跳过重试，直接切兜底 |
| 同上 | `stop_after`（默认 0 = 关） | `retryable=False` + `should_fallback=False`：结束这一轮 |
| `rate_limit`（429） | `rotate_after`（默认 2） | 请求轮换凭据，保持 429 可重试的原语义 |
| 其余（401/403/402/400/404、内容策略、TLS、本机路径/权限/导入错误） | — | 返回 `None`，交回内建分类器 |

路由键是 `provider|model|host`，host 从异常的 request URL 取——让“死掉的 base_url”和“健康的兜底 base_url”
各算各的。传输类失败连续 3 次才升级；抖动不触发。

整链熔断：`chain_window_seconds`（默认 90 秒）内多条路由被降级，说明整条兜底链都是坏的，
直接停手，不再逐个试。

## 副作用（必须知道）

- 触发升级的那次失败会被标成**不可重试**：如果兜底链已用完，这一轮会直接以报错结束，而不是继续等。
  取舍是有意的——宁可 30 秒给一个明确失败，也不要盲等十分钟。
- 一小时最多干预 `max_actions_per_hour` 次，超出后只记录。
- 窗口计数在进程内，重启清空；账本落盘可审计。
- 只写自己的数据目录，不联网、不读凭据；钩子异常一律 fail-open。

## 配置

`config.yaml` → `plugins.entries.ops-failover`：

| 键 | 默认 | 说明 |
|---|---|---|
| `enabled` | true | 总开关 |
| `window_minutes` | 10 | 每路由的滚动窗口 |
| `rotate_after` | 2 | 第几次 429 后轮换凭据 |
| `fallback_after` | 3 | 第几次传输失败后切兜底 |
| `stop_after` | 0 | 第几次失败后结束本轮（0=不启用） |
| `max_actions_per_hour` | 20 | 断路器 |
| `max_tracked_routes` | 50 | 最多跟踪多少路由 |
| `chain_stop_after` | 3 | 短窗口内几条路由降级后整链熔断（0=关） |
| `chain_window_seconds` | 90 | 熔断的判定窗口 |

## 命令

```
/failover            近 24 小时 + 当前窗口计数
/failover 2h         任意小时/天（6h、7d、all）
/failover status     只看当前窗口计数
/failover clear      清账本与窗口计数
```

## 测试

```bash
cd tests
python unit_failover.py     # 28 项：分类、本机错误、路由键、决策四态、熔断、账本并发
```

## 许可

MIT。
