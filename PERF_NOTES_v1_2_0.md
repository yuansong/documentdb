# DocumentDB 1.2.0 性能优化（分支 `v1_2_0`）

本分支在 upstream `a5f1636` 之上只包含性能相关的改动，共 4 个提交与 1 项运行期配置。
功能、协议、认证、错误语义均未改变：响应字节与改动前逐字节一致，每次请求的 PostgreSQL 往返次数不变。

## 1. 提交清单

| 提交 | 标题 | 文件 | 作用 |
| --- | --- | --- | --- |
| `fbf9b05` | perf(gateway): coalesce wire header+BSON body into one vectored write | `pg_documentdb_gw/documentdb_gateway_core/src/responses/writer.rs` | 热路径原本用两次 `write_all`（16 字节报文头 + BSON 体），每次都会单独进入异步写路径并可能各自触发一次 socket 写。改为一次 `write_vectored([header, body])`，并按需处理部分写与不支持 vectored 的 sink。每个响应省掉一次异步写 + 一次系统调用。 |
| `a873491` | perf(gw_host): size the gateway async runtime from the machine, not one thread | `pg_documentdb_gw_host/src/bgworker.rs` | 网关以 PostgreSQL bgworker 形式运行，原来把 tokio runtime 写死为 `worker_threads(1)`，所有客户端连接的 socket I/O 都由单线程轮询。改为按 `available_parallelism()` 取线程数，并支持 `DOCUMENTDB_GW_WORKER_THREADS` 覆盖。 |
| `ad66e3b` | perf(gw_host): size the Tokio pool to half the cores, not all of them | `pg_documentdb_gw_host/src/bgworker.rs` | 网关线程与执行其查询的 backend 抢同一批核，按全核数会超订。默认改为 `核数/2`（`div_ceil(2)`），16 vCPU 机器上 8 线程为实测峰值。仍可用 `DOCUMENTDB_GW_WORKER_THREADS` 覆盖。 |
| `a2924fa` | feat(pg_documentdb): define enableWriteProcedures GUCs so gateway dynamic config can read them | `pg_documentdb/src/configs/feature_flag_configs.c` | 网关的 `DynamicConfiguration` 只从 `pg_settings` 读 `documentdb.` 前缀、剥掉前缀后取 `enableWriteProcedures` / `enableWriteProceduresWithBatchCommit`，但 1.2.0 后端从未定义这两个 GUC，导致 `ALTER SYSTEM SET` 静默失败、写路径开关根本不可能生效。补上两个 GUC（默认值与网关一致：`on` / `off`）。 |

## 2. 运行期配置（非代码，需部署时手工设置）

网关默认通过 TCP 127.0.0.1 连接 PostgreSQL。把连接改为 Unix socket 不需要改一行代码——tokio-postgres 的 `Config::host()` 对以 `/` 开头的值会自动走 `host_path`：

```json
// SetupConfiguration.json
"PostgresHostName": "/tmp"
```

前提是 `pg_hba.conf` 中存在 `local all all trust`（或对应的认证行）。
该配置在 C=80 读负载上实测 90,466 → 97,569 ops/s（+7.9%），平均延迟 872 → 805 µs；收益来自本地回环收包时的软中断与唤醒不再与网关线程同核竞争。

## 3. 构建与部署

```bash
export RUSTUP_HOME=$HOME/.rustup CARGO_HOME=$HOME/.cargo PATH=$CARGO_HOME/bin:$PATH
export CARGO_NET_OFFLINE=true

# 网关（以运行 PostgreSQL 的同一用户构建，pgrx 绑定沿用该用户的 ~/.pgrx）
cd pg_documentdb_gw_host
cargo +1.97 build --release --features pg16 --no-default-features

# 产物部署到 PG 的库目录，两个文件名都要覆盖（库里两种名字都被 dlopen 过）
PGDIR=$(pg_config --pkglibdir)
cp target/release/libpg_documentdb_gw_host.so $PGDIR/
cp target/release/libpg_documentdb_gw_host.so $PGDIR/libpg_documentdb_gw_host.so

# 后端 GUC 补丁
make -C pg_documentdb -j"$(nproc)"
cp pg_documentdb/pg_documentdb.so $PGDIR/
```

重启 PostgreSQL 后生效。设置 `DOCUMENTDB_GW_WORKER_THREADS=<n>` 可覆盖默认线程数（默认 `核数/2`）。

## 4. 实测结果

环境：16 vCPU / 7 GB，PostgreSQL 16.15，YCSB 0.17 mongodb binding，`recordcount=20000`、`maxexecutiontime=20s`、每档 3 轮取中位，C=1 单 JVM、C≥20 双 JVM 对半分摊，服务互斥。

- **优化前（PRE）**：官方 v1.2-0 原始行为 —— 单 tokio worker、两次 `write_all`、TCP 127.0.0.1。
- **优化后（OPT）**：本分支全部 4 个提交 + Unix socket。

吞吐中位（ops/s）：

| 并发 | workloada 前 | workloada 后 | 提升 | workloadb 前 | workloadb 后 | 提升 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C=1 | 4,258 | 4,111 | -3.5% | 5,831 | 5,562 | -4.6% |
| C=20 | 24,099 | 56,135 | +132.9% | 23,893 | 70,828 | +196.4% |
| C=40 | 23,045 | 62,929 | +173.1% | 23,303 | 79,611 | +241.6% |
| C=80 | 22,094 | 54,116 | +144.9% | 22,506 | 81,221 | +260.9% |
| C=100 | 21,410 | 51,730 | +141.6% | 22,312 | 80,191 | +259.4% |
| C=120 | 20,903 | 50,807 | +143.1% | 21,656 | 59,260 | +173.6% |

结论：

- C≥20 的提升几乎是**单线程 runtime 天花板**被移除的结果。官方原版在 C≥20 后稳定卡在约 22k ops/s，改成 `核数/2` 后平台抬到 50~81k，此时 PostgreSQL 自身成为限制层（同机 pgbench 扩展查询上限约 83.5k tps）。
- C=1 有 -3.5% / -4.6% 的轻微回退：单连接无法分摊到多个 runtime 线程，多线程调度只带来开销。若部署场景以单连接为主，可用 `DOCUMENTDB_GW_WORKER_THREADS=1` 复原。
- 写合并与 Unix socket 在小并发档对延迟的改善更明显，吞吐上的独立贡献在 CPU 饱和点被掩盖。
- workloadb C=100 → C=120 出现 80.2k → 59.3k 的台阶（3 轮稳定复现）；同机在 `max_wal_size=10GB` 下的历史基线同档为 84.3k，怀疑与压测期间把 `max_wal_size` 降到 2GB、每轮 CHECKPOINT 以及数据集膨胀有关，尚未定论，不影响上表其余档位。

## 5. 其他

- 共享内存传输通道（`shm_channel` 内核补丁 + `shmem_transport`）为已放弃的探索方案，不在本分支，历史保留在 `v1.2-shm`、`pg16-kernel-shm` 分支。
- `perf-0.113` 分支包含同一组优化在 0.113 版本上的移植。
- MongoDB 8.0 对比数据来自同口径历史基准，未在本轮重测。
