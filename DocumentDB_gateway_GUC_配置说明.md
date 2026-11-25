# DocumentDB Gateway 配置说明（GUC 参数与默认值）

> 适用范围：`pg_documentdb_gw_host`（gw_host 后台 worker，随 PostgreSQL 启动）。
> 独立部署的 standalone gateway 二进制没有 GUC 环境，仍通过 SetupConfiguration JSON 与环境变量配置。
> 基线：`gw-multi-instance-1.2` 分支（本分支起环境变量通道全部迁移为 GUC 优先）。

## 1. 参数总表

| GUC 参数 | 类型 | 默认值 | 生效方式 | 说明 |
|---|---|---|---|---|
| `documentdb_gateway.database` | string | 无（必填） | 重启 | gateway 连接并创建扩展的数据库 |
| `documentdb_gateway.setup_configuration_file` | string | 无（必填） | 重启 | SetupConfiguration JSON 文件路径 |
| `documentdb_gateway.gateway_instances` | int | **0**（未配置） | 重启 | 共享同一监听端口的 gateway 实例数；>1 时启用 SO_REUSEPORT，上限 64 |
| `documentdb_gateway.async_runtime_worker_threads` | int | **0**（未配置） | 重启 | 每实例 tokio worker 线程数 |
| `documentdb_gateway.data_pool_connections` | int | **0**（未配置） | 重启 | 每实例每凭据的数据连接池上限 |
| `documentdb_gateway.cpu_affinity` | string | **NULL**（未配置） | 重启 | 绑核范围，如 `3` 或 `0-7`；非法值忽略（不绑核） |
| `documentdb_gateway.log_filter` | string | **NULL**（未配置） | 重启（不热加载） | tracing 过滤器，语法同 `RUST_LOG`；启动时读取一次 |
| `documentdb_gateway.tls_state_dir` | string | **NULL**（未配置） | 重启 | 自动生成 TLS 证书（cert.pem/pkey.pem）的存放目录 |

所有 GUC 均为 `GucContext::Postmaster` + `SUPERUSER_ONLY`：
修改后需**重启 PostgreSQL**（与原环境变量的生效语义一致）；仅超级用户可设置。
所有值在 bgworker 初始化阶段一次性快照，不在请求路径上读取 GUC（避免 tokio 线程 FFI 调用）。

## 2. 配置优先级（逐项）

| 配置项 | 优先级链（高 → 低） |
|---|---|
| 实例数 | **GUC** → `DOCUMENTDB_GW_INSTANCES` → 默认 1 |
| worker 线程 | **GUC** → JSON 键 `AsyncRuntimeWorkerThreads` → `DOCUMENTDB_GW_WORKER_THREADS` → 推导（核数/2 按实例数平分，为 PG 维护进程预留） |
| 数据池连接 | **GUC** → `DOCUMENTDB_GW_DATA_POOL_CONNECTIONS` → 推导（max_connections 公平份额，受 线程数×2 上限约束，下限 1） |
| CPU 绑核 | **GUC** → `DOCUMENTDB_GW_CPU_AFFINITY` → 不绑核 |
| 日志过滤 | **GUC** → `RUST_LOG` → 默认 `info` |
| TLS 目录 | **GUC** → `DOCUMENTDB_TLS_STATE_DIR` → `/var/lib/documentdb-gateway/tls`（不可写时回退 XDG 用户目录、再回退临时目录） |

说明：

- int 类 GUC 的 `0` 表示「未配置」，自动落入下一级通道；这保证**不配置任何 GUC 时行为与迁移前完全一致**。
- `async_runtime_worker_threads` 的 GUC 优先于 JSON 键，是唯一一处新增的最高优先级通道；GUC 未设置时，JSON 与 env 的相对顺序保持原状（JSON > env）。
- 环境变量通道全部保留，作为容器 / K8s / 既有部署脚本的兼容层，无感迁移。

## 3. 推导默认值参考（24 vCPU 实例实测）

| 配置项 | 推导值 |
|---|---|
| worker 线程 | `cores/2 = 12`（且不超过 cores − PG 预留核） |
| PG 预留核 | `min(12, max(1, cores/4))` = 6（含 4 维护进程 + 3 autovacuum + 5 pg_cron 上限，封顶核数 1/4） |
| 数据池连接 | `min((max_connections − K×system_budget)/K, threads×2)` = `min(993, 24)` = **24**（max_connections=1000、K=1） |
| 实例数 | 1 |

## 4. 配置示例（postgresql.conf）

```ini
# 基础（必填，与迁移前一致）
documentdb_gateway.database = 'postgres'
documentdb_gateway.setup_configuration_file = '/path/to/SetupConfiguration.json'

# 可选：多实例 + 资源隔离
documentdb_gateway.gateway_instances = 2            # 两个 gateway 共享 10260 端口
documentdb_gateway.async_runtime_worker_threads = 8 # 每实例 8 个 tokio worker
documentdb_gateway.data_pool_connections = 33       # 每实例每凭据最多 33 个后端连接
documentdb_gateway.cpu_affinity = '0-7'             # gateway 线程绑定 0-7 号核

# 可选：诊断与 TLS
documentdb_gateway.log_filter = 'documentdb_gateway_core=debug'  # 打开每请求分解日志
documentdb_gateway.tls_state_dir = '/var/lib/postgresql/gw_tls'
```

验证：

```sql
SHOW documentdb_gateway.async_runtime_worker_threads;
SELECT name, setting, source FROM pg_settings WHERE name LIKE 'documentdb_gateway.%';
-- source='configuration file' 表示来自 postgresql.conf；source='default' 表示未配置（走回退链）
```

启动日志（每次实例启动一次性输出，用于与 standalone 对比）：

```text
GW-HOST: gw0/2 tokio worker threads: 8 (...)
GW-HOST: gateway affinity cpus 0-7 -> rc=0 (pinned)
... worker_thread_source="guc (documentdb_gateway.async_runtime_worker_threads)"
... data_pool_connections=33 data_pool_override_set=true cpu_affinity="0-7"
```

`worker_thread_source` 取值：`guc (...)` / `setup configuration (AsyncRuntimeWorkerThreads)` / `env (DOCUMENTDB_GW_WORKER_THREADS)` / `derived (...)`，来源可追溯。

## 5. 兼容性

- **旧行为不变**：不设置任何新 GUC 时，所有解析路径与迁移前逐字节一致（已在本机 24 vCPU 环境做基线对照验证）。
- **env 部署脚本无需改动**：GUC 未配置时 env 继续生效。
- **standalone gateway 不受影响**：其 `RUST_LOG` / `DOCUMENTDB_TLS_STATE_DIR` 等 env 通道保持原样。
- 多实例（instances > 1）不支持 `unix_socket_path` 监听（SO_REUSEPORT 仅限 TCP），配置了会拒绝启动并提示。

## 6. 排查要点

1. GUC 改了没生效 → Postmaster 上下文必须重启 PostgreSQL，`ALTER SYSTEM SET` 不即时生效。
2. 来源存疑 → `pg_settings.source` + 启动 dump 的 `worker_thread_source` 双重证据。
3. 崩溃取证 → panic hook 仍强制输出完整栈（不依赖 `RUST_BACKTRACE`，后者保持关闭以保护请求路径性能）。
