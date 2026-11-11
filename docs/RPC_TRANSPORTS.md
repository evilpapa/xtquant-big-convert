# RPC 传输层

RPC 使用 `redis`；`shm` 是占位实现。两端的 `transport` 配置必须一致。

- Redis 使用请求队列及响应通道；安装 `xtquant-big-convert[redis]`。

服务端在 `bigqmt_signal_trader_local_config.py` 配置 `BIGQMT_REDIS_CONFIG`，客户端在 `bigqmt_signal_trader_client_config.py` 配置相应传输。调用 `bigqmt-init` 可生成 Redis 配置。

```python
BIGQMT_REDIS_CONFIG = {
    "transport": "redis",
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "rpc_background_threads": False,
    "schedule_adjust": True,
}
```

部署前请确认券商要求。部署和交易环境须符合券商要求。

## 策略名与重启恢复

RPC 通道与策略归属存储是两件事。配置 Redis 时，身份记录经
Redis 读取；仅切换通道无法解决 QMT 内置 Redis 的 `Sensitive Data Detected, Forbidden!`
响应过滤错误（#393）。身份值现使用无数字编码，解码后原有字段不变；仍存活的旧 JSON
若被过滤，会在 Redis 内原子迁移，保留原内容及剩余过期时间。此兼容路径需要 Redis
允许 `EVAL`、`GET`、`SETRANGE`；权限不足时原记录保留，错误可从 `probe_order_identity`
的 `lookup_error` 查看。

没有配置 Redis 时，服务端在下单前同步追加本地身份 journal，并在启动时恢复七天内的
策略名。默认目录为 QMT 的 `python/state/order_identity`，不可写时回退到用户目录下
`.cache/bigqmt/order_identity`。可在启动 QMT 前设置 `BIGQMT_ORDER_IDENTITY_DIR` 指定
固定目录；文件按账户和账户类型隔离。重装、换运行用户或迁移终端时须保留此目录。
`probe_order_identity` 的 `local_persistence_directory`、`local_persistence_error`
分别显示实际路径及持久化错误。journal 写入失败时当前进程仍使用内存归属，重启恢复可能不完整。

该修复不改变 `order_remark`。身份记录里的 `user_order_id` 就是提交时传入的 remark，
同一 remark 的首个非空策略名仍优先。无 Redis 时的落盘行为从升级后生效；旧进程已丢失且
没有持久记录的归属无法重建。同步新包和顶层策略文件后，须重启策略一次。
