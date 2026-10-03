# HPC 作业抢占与检查点恢复服务

实现调度器回收节点场景下的**抢占纪元签发、内容寻址检查点、全量校验释放、
同代际恢复与输出租约栅栏**，以及集群下线/搬迁时的**可恢复检查点迁移
（跨内容存储）**。纯 Python 标准库，无外部依赖。

## 核心不变量

| # | 不变量 | 实现位置 |
|---|--------|----------|
| 1 | 回收前签发**抢占纪元**（单调 epoch/generation + HMAC 签名令牌）；重复抢占通知幂等，不轮换令牌 | `epochs.py`、`jobs.py::preempt` |
| 2 | 检查点分片为**内容寻址**（`sha256(payload)`），相同分片跨代际天然去重 | `storage.py::ContentStore` |
| 3 | 期望分片集合在开检查点时由「进程集合 × 每进程分片数」固定；提交时**逐个从 CAS 重读重算哈希**，缺片/损坏一律拒绝，检查点停留在 `PENDING`，绝不提前可用 | `checkpoints.py::commit` |
| 4 | **只有全部校验完成**（`COMPLETE`）才允许确认释放；不完整只能 `abandon`（`INCOMPLETE`/`ABORTED`） | `jobs.py::confirm_release/abort_epoch` |
| 5 | **恢复必须使用同一检查点代际**：恢复时再次全量重校验、核对进程集合、回传每个分片输入位点，校验通过才签发**新的输出租约** | `jobs.py::restore`、`leases.py` |
| 6 | 抢占瞬间吊销旧租约；旧进程迟到写入（旧 generation/已 REVOKED 令牌）返回 `412 FenceViolation`；输出按 `(generation, output_id)` 幂等去重 —— 无双重输出 | `leases.py::authorize`、`storage.py::OutputLog` |
| 7 | 节点失联：节点与 rank 标记 `LOST`，其分片不被接受、提交被阻塞；心跳恢复后 incarnation+1，补齐方可整体提交 | `jobs.py::mark_node_lost/heartbeat` |
| 8 | 新一轮抢占使旧的未完成检查点 `SUPERSEDED`；旧纪元令牌不能写新代际 | `checkpoints.py`、`jobs.py` |
| 9 | **结果提交授权失败统一处理**：缺失/伪造/超期/撤销/跨作业/旧代际凭证一律返回稳定错误（固定状态码+`reason`+固定消息，不回显凭证），并写入一条**顺序化安全事件**（全局单调 seq，不含凭证原文）；授权在写入前终止，重复并发请求不增加结果条目 | `leases.py::authorize`、`jobs.py::_deny` |
| 10 | **事件溯源持久化**：所有状态变更（含安全事件、输出条目、恢复报告、GC 计划/删除、迁移任务/逐项传输）顺序追加到 `data_dir/journal.jsonl`，blob 原子落盘；服务重启按 seq 重放重建，页面与验证报告仍可查询，旧令牌仍被栅栏，同密钥重新派生 | `persistence.py`、`jobs.py::_replay` |
| 11 | **可审计空间回收**：GC 先生成带 journal 水位、保留代际、引用指纹、预计释放量和候选明细的计划；可锁定/解锁代际；计划生成后引用关系变化则拒绝执行；上传/恢复/回收共享全局引用临界区，绝不删除仍可访问数据；RUNNING 计划崩溃重启后可继续，每个删除动作和跳过/恢复原因均可查询 | `gc.py`、`jobs.py::create_gc_plan/execute_gc_plan` |
| 12 | **可恢复检查点迁移**：导出端冻结对象清单并临时保留涉及载荷（GC 不得回收）；接收端按摘要复用已有对象；逐项事件支持断点续传与重启续跑；**全部内容校验通过才一次性发布可恢复代际**；清单变化或摘要错误绝不发布；取消/失败释放临时保留并清理目标端半成品，不妨碍正常空间回收 | `migration.py`、`jobs.py::create_migration/run_migration` |

## 状态机

```
作业:   RUNNING ──preempt──▶ PREEMPTING ──release(仅COMPLETE)──▶ RELEASED
                                  │                                   │
                                  └──abandon──▶ ABORTED              │
                                      ▲                              │
        RELEASED ──restore(同代际+重校验+新租约)──▶ RUNNING ◀─────────┘

检查点: PENDING ──全部分片重校验通过──▶ COMPLETE      (可重复 commit: 幂等)
            ├── 缺片/损坏: 提交拒绝, 保持 PENDING
            ├── 新纪元接管: SUPERSEDED
            └── 宽限期耗尽放弃: INCOMPLETE

迁移任务: PLANNED ──run──▶ TRANSFERRING ──清单复核+目标端全量校验──▶ PUBLISHED
             │                 │ (逐项事件, 中断/重启后续传)
             │                 └── 缺片/摘要错误/清单变化 ──▶ FAILED
             └──cancel──▶ CANCELLED        (FAILED/CANCELLED 均释放临时保留,
                                            清理目标端半成品, 不产生可恢复代际)
```

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v        # 69 个测试, 含 GC/迁移/并发竞争与重启持久化

# 端到端演示 (打印恢复验证报告 Markdown, 含跨存储迁移演示)
python3 demo.py
# 持久化演示 (状态/安全事件落盘, 重跑自动重放)
python3 demo.py --data-dir ./hpc-data
# 演示后继续提供 HTTP 服务, 浏览器打开节点进度页
python3 demo.py --serve --port 8080
#   http://127.0.0.1:8080/ui/jobs/climate-7

# 空服务 (纯内存, 重启不保留)
python3 -m hpc_cr            # 默认 127.0.0.1:8080
# 持久化服务 (事件日志 + blob + 自动生成 0600 签名密钥)
python3 -m hpc_cr --data-dir ./hpc-data
# 注册迁移目标内容存储 (可重复; 重启后需以同样配置重新提供)
python3 -m hpc_cr --data-dir ./hpc-data --destination archive=./archive-blobs
# 也可用环境变量 HPC_CR_SECRET 固定签名密钥 (重启后旧令牌才可继续验证)
```

## 作业 API

所有写操作为 `POST /api/jobs/{id}`，JSON body 用 `action` 区分；
凭证走请求头：`X-Epoch-Token`（抢占纪元令牌）、`X-Lease-Token`（输出租约令牌）。
二进制数据以 `data_b64`（标准 base64）传递。

| 方法/路径 | 说明 |
|---|---|
| `POST /api/jobs` | 创建作业（world_size、shards_per_rank、rank→node 放置） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 作业列表 / 快照（纪元、宽限剩余、当前检查点、租约） |
| `action=preempt` | 签发抢占纪元；重复通知幂等，`force_new_epoch:true` 表示新一轮抢占 |
| `action=heartbeat` | 节点心跳（LOST→UP，进程 incarnation+1） |
| `action=mark_node_lost` | 标记节点失联，阻塞其分片 |
| `action=progress` | 上报 rank 输入位点 |
| `action=upload_shard` + `X-Epoch-Token` | 上传分片（rank/index/node/input_pos/data_b64），重复同内容幂等 |
| `action=commit_checkpoint` + `X-Epoch-Token` | 全量重校验提交；缺片/损坏返回 409 |
| `action=confirm_release` + `X-Epoch-Token` | 确认释放；非 COMPLETE 返回 409 |
| `action=abandon` | 宽限耗尽放弃，检查点 INCOMPLETE |
| `action=append_output` + `X-Lease-Token` | 提交输出；旧代际/吊销租约 412；同 output_id 幂等 |
| `action=restore` `{generation}` | 同代际恢复验证，返回恢复验证报告 + 新租约 |
| `action=pin_generation` `{generation, unpin, reason}` | 锁定/解锁需要保留的检查点/输出代际；变更会推进引用水位，使旧 GC 计划过期 |
| `POST /api/gc/plans` | 生成 GC 计划（`policy`、`min_retain_generation`、`retain_generations`、`include_pins`），返回预计释放量、候选 blob、保留原因、引用指纹和水位，不删除数据 |
| `POST /api/gc/plans/{id}` `{action:"execute"}` | 按计划执行；水位推进、引用变化或物理 blob 集合变化时返回 409 并把计划标记 `STALE` |
| `GET  /api/gc/plans` / `GET /api/gc/plans/{id}` | 全部计划 / 单计划查询：预计/实际删除项、字节、跳过和崩溃恢复原因、最终状态 |
| `GET  /api/jobs/{id}/gc/pins` | 查询作业已锁定代际 |
| `POST /api/migrations` | 创建迁移任务（`job_id`、`generation`、`destination`，可选 `task_id`）；同（作业, 代际, 目标）的活动任务重复创建幂等返回同一任务 |
| `POST /api/migrations/{id}` `{action:"run"}` | 继续/执行迁移（可选 `max_items` 分块）；中断/重启后用同一 id 续传，全部校验通过才发布 |
| `POST /api/migrations/{id}` `{action:"cancel"}` | 取消活动任务：释放临时保留，清理目标端半成品 |
| `GET  /api/migrations` / `GET /api/migrations/{id}` | 全部任务（含已注册目标存储）/ 单任务查询：计划总量、已传输项、复用项、失败项、最终状态和原因、逐项明细 |
| `GET  /api/jobs/{id}/migrations` | 本作业迁移任务与已发布可恢复代际记录 |
| `GET  /api/jobs/{id}/progress.json` | 进度页聚合数据（节点/进程/分片矩阵/输出/审计） |
| `GET  /api/jobs/{id}/checkpoints` | 全部代际检查点 |
| `GET  /api/jobs/{id}/outputs` | 输出条目（按代际隔离） |
| `GET  /api/jobs/{id}/security-events` | **授权失败安全事件**（顺序化 seq/原因/绑定字段，不含凭证原文） |
| `GET  /api/jobs/{id}/restore/report` | 最近一次恢复验证报告（JSON，含截至当前的安全事件列表） |
| `GET  /ui/jobs/{id}` | **节点进度页面**（2s 自动刷新，含安全事件表） |

### 授权失败稳定错误 (`append_output` 等凭证接口)

错误响应固定为 `{error, reason, message}`，同一类拒绝永远得到同一三元组，
消息不随请求/令牌变化，不回显任何凭证原文：

| 场景 | HTTP | error | reason |
|---|---|---|---|
| 缺失凭证头 | 401 | invalid_token | `missing_token` |
| 伪造/篡改/格式损坏令牌 | 401 | invalid_token | `bad_signature` / `malformed_token` / `bad_token_version` / `undecodable_payload` |
| 令牌类型错用（epoch 充当 lease） | 401 | invalid_token | `wrong_token_type` |
| 已签发但租约 ID 不可识别 | 401 | invalid_token | `unknown_lease` |
| 旧代际凭证迟到写入 | 412 | fence_violation | `stale_generation` |
| 跨作业凭证 | 412 | fence_violation | `cross_job_token` |
| 抢占时已撤销租约 | 412 | fence_violation | `lease_revoked` |
| 超过租约/令牌有效期 | 412 | fence_violation | `lease_expired` |
| 纪元令牌跨作业/旧纪元 | 412 | fence_violation | `epoch_cross_job_or_epoch` |

每次拒绝同步写入一条 `security_authz_denied` 事件：包含全局单调 `seq`、
`credential`（`output_lease`/`epoch_token`）、`operation`、`reason`、
`http_status`、`expected_generation` 与仅可公开的绑定字段 `claimed`
（job/generation/lease id），**绝不包含令牌串或签名**。授权在校验通过前
终止，blob 与 OutputLog 不发生任何写入，故重复并发的无效/重复请求不会
增加结果条目（合法重复提交仍按 `(generation, output_id)` 幂等）。

其余错误映射：`400` 请求格式、`404` 对象不存在、
`409` 状态冲突（缺片提交/重复分片/状态机）。

## 恢复验证报告

`restore()` 与 `/restore/report` 返回结构化报告（`RestoreReport`），并可用
`report.to_markdown()` 渲染，包含：清单摘要、分片重算哈希结果（逐个列出失败
分片）、每个分片的**输入位点 seek 表**、旧/新进程放置与迁移 rank、新输出租约、
**安全事件表（授权拒绝证据，读取时补齐到最新）**、警告；任一检查项失败时
`success=false` 且作业保持 `RELEASED`，不会带着坏检查点运行。报告随事件日志
持久化，服务重启后仍可查询。

## 空间回收 (GC) 语义

回收采用 **计划 → 锁定/复核 → 执行 → 查询** 的显式流程:

- 计划生成在全局引用临界区和全部作业锁内完成, 记录:
  `watermark_seq`（引用类事件水位）、全部逻辑引用关系的
  `reference_fingerprint`、每代际保留原因、物理 blob 清单、候选 blob、
  `estimated_reclaim_items/bytes`。计划创建本身只追加 `gc_plan_created`。
- 默认可恢复策略 (`recoverable`) 保留当前抢占/PENDING 检查点、
  已释放待恢复检查点、运行中输出代际及其输出; `all_complete`
  保留所有 COMPLETE 检查点; `watermark + min_retain_generation`
  保留指定水位之后的代际。`pin_generation` 可持久化锁定任意代际。
- 执行时重新验证水位、引用指纹和物理集合: 计划生成后发生上传、
  抢占、释放、放弃、恢复、新输出、代际 pin/unpin 或出现未纳管 blob,
  一律返回 409，计划转为 `STALE`，不会删除任何数据。
- 每个 blob 删除成功后独立追加 `gc_blob_deleted`；计划完成追加
  `gc_run_finished`，包含实际删除项/字节、`skip_reasons` 和最终状态。
  若崩溃发生在物理 unlink 与删除事件之间，重启后继续同一计划时该项计入
  实际删除，并记录恢复原因 `already_deleted_after_crash`。
- 上传分片、提交输出、恢复和 GC 扫描/执行共享全局引用锁，因此不会看到
  "blob 已落盘但元数据引用尚未建立" 的中间态；正在执行的计划会阻止生成
  新计划，COMPLETED 计划重复执行幂等。
- 迁移任务的临时保留 (migration hold) 进入 GC 快照与引用指纹：持有期间
  涉及载荷一律视为 retained，此前生成的计划因水位/指纹变化被拒绝执行。

## 检查点迁移 (跨内容存储)

集群下线或搬迁时，把一个 **COMPLETE** 检查点及其同代际输出迁移到另一套
内容存储 (destination，经 `register_destination` / `--destination` 注册，
属配置项不入事件日志)。流程为 **冻结清单 → 断点续传 → 校验后原子发布**：

- **冻结清单 + 临时保留**：创建时在全局引用临界区内固定对象清单（检查点
  分片 + 同代际输出，按 blob 去重、引用合并）并计算 `manifest_digest`；
  清单内载荷登记为 migration hold，GC 不得回收（见上节）。
- **按摘要复用**：目标端同为内容寻址存储，已存在的 blob 写入即去重，记为
  复用项 (`REUSED`)；新写入记为已传输项 (`TRANSFERRED`)。源端读出时重算
  sha256，写入后在目标端再次 verify —— 任何摘要错误立即失败。
- **断点续传**：每个清单项的传输结果独立追加 `migration_item_transferred`
  事件；传输中断（可用 `max_items` 分块）、重复 `run`、进程重启后用同一
  `task_id` 继续，已完成项跳过，目标端已有内容（含崩溃窗口内未落盘事件
  的写入）按复用处理。同（作业, 代际, 目标）重复创建幂等返回同一任务。
- **原子发布**：全部项到位后重新计算清单摘要与冻结值比对（期间追加的同
  代际输出等**清单变化**导致 `manifest_changed` 失败），并对目标端逐项
  重算哈希；全部通过才追加**一条** `migration_published` 事件，发布可
  恢复代际记录（`GET /api/jobs/{id}/migrations` 的 `published`）。
  清单变化或摘要错误**绝不**产生该记录。
- **释放与清理**：进入 `PUBLISHED`/`FAILED`/`CANCELLED` 即释放临时保留，
  不妨碍正常空间回收；失败/取消还会尽力删除目标端由本任务新写入且不被
  其他活动/已发布任务引用的 blob（复用项原本就存在，绝不删除），清理
  结果计入任务的 `cleanup` 字段。
- 进度页与 API 展示：计划总量 (`planned_items/bytes`)、已传输项
  (`transferred_items/bytes`)、复用项 (`reused_items/bytes`)、失败项
  (`failed_items`)、待传输 (`pending_items`)、最终状态与原因
  (`state`/`state_reason`/`cancel_reason`/`error`) 及逐项明细。

## 并发正确性

- 每作业一把 `RLock`；另有全局引用锁、GC 状态锁与迁移锁，固定顺序为
  reference → GC state → 各 Job.lock（按 job_id 排序），迁移操作为
  reference → migration → Job.lock（与 GC 锁不嵌套）；CAS 存储独立细锁。
  迁移保留快照只在引用临界区内整体替换，GC 在引用临界区内读取，无需再
  加迁移锁。
- 测试 `tests/test_concurrency_api.py` 覆盖：
  - 旧进程迟到写入与恢复后新进程并发提交相同 `output_id` → 旧写入 412、恰好一次落盘；
  - 8 线程并发重复 output / shard → 只有一次 delivered/stored；
  - 未补齐期间并发提交 6 次全部失败，补齐后才 COMPLETE；
  - 失联恢复与提交循环并发 → COMPLETE 最多出现一次且在补齐之后。
- 测试 `tests/test_gc.py` 覆盖：
  - 计划预计释放量/保留原因/候选明细，执行后实际删除项和字节；
  - 代际 pin 与代际水位；计划后恢复/输出/pin 变化 → STALE 且拒绝执行；
  - 上传、恢复与 GC 并发时仍可访问的数据绝不被删除；
  - “物理删除已成功但事件未落盘”的崩溃重启续跑与查询接口/作业页面展示。
- 测试 `tests/test_migration.py` 覆盖：
  - 冻结清单/计划总量、按摘要复用、分块续传、重复创建与重复 run 幂等；
  - 清单变化与源端摘要错误/缺片 → 任务 FAILED 且不产生可恢复代际；
  - 取消/失败释放临时保留、清理目标端半成品、不妨碍后续 GC；
  - 迁移保留阻止 GC 回收、既有计划因迁移开始而 STALE、GC 与迁移并发安全；
  - 进程重启后续跑同一任务并恰好发布一次，目标存储崩溃窗口写入按复用处理；
  - HTTP API 与作业页面的迁移进度展示。
- 测试 `tests/test_authz_security.py` 覆盖：
  - 缺失/伪造/错类型/撤销/超期/跨作业/旧代际凭证的稳定错误分类与无回显；
  - 每次拒绝一条顺序化安全事件（seq 唯一递增、无凭证原文）；
  - 8 线程并发无效/混合请求不增加结果条目，合法重复恰好一次；
  - 落盘后新建管理器（模拟重启）：页面、`/security-events`、恢复验证报告
    （JSON 与 Markdown）仍可查询事件，输出/检查点/blob 完整且旧凭证仍被拒。

## 代码结构

```
hpc_cr/
  security.py     # HMAC 签名令牌、规范化 JSON、sha256 CAS 原语
  persistence.py  # 顺序化事件日志 Journal、持久化 blob 后端、签名密钥加载
  epochs.py       # 调度器、抢占纪元 (epoch/generation/deadline)
  storage.py      # 内容寻址 blob 存储 (内存/磁盘) + 输出去重日志
  checkpoints.py  # 分片、检查点状态机、全量重校验提交、重放回填
  leases.py       # 输出租约、代际栅栏、授权失败稳定归类
  gc.py           # GC 引用快照、保留代际、候选与计划视图
  migration.py    # 迁移任务状态机、冻结清单/摘要、任务视图
  jobs.py         # Job/JobManager 聚合根、恢复验证报告、GC/迁移编排、事件溯源/重放
  api.py          # stdlib ThreadingHTTPServer JSON API
  ui.py           # 节点进度页面 HTML 渲染 (含安全事件、GC 计划与迁移任务表)
tests/            # 69 个单元/并发/HTTP/GC/迁移/重启持久化测试
demo.py           # 端到端场景脚本 (可选 --data-dir 持久化)
```

## 持久化与重启语义

`data_dir` 布局：

- `journal.jsonl`：只追加事件日志，每行一个带全局单调 `seq` 的 JSON；
  写入 `flush + fsync`，重启按 seq 重放重建作业/检查点/租约/输出/审计/
  安全事件/GC 计划与删除进度/迁移任务与逐项传输进度。崩溃最多丢失未落盘
  的最后一行（该行对外也不可见）；GC 删除若先于事件落盘，重放后按同一
  计划安全续跑；迁移传输若先于事件落盘，续跑时目标端去重按复用处理。
- `blobs/<sha256>`：分片与输出载荷，临时文件 fsync + 原子 rename。
- `signer.key`：首次启动自动生成的 HMAC 密钥（0600）；也可用
  `HPC_CR_SECRET` 覆盖。令牌自描述不入库，重放时以同密钥重新派生，
  因此旧令牌重启后仍能被验证或被栅栏拒绝。

不给定 `data_dir` 时全部退化为内存实现，接口行为一致。
