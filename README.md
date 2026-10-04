# tracing-pipeline-go

进程内的追踪采集与导出管道（Go，无第三方依赖）。它把来自不同来源的追踪上下文
归一为统一链路模型，做**基于父级、按链路冻结**的采样，并通过**有界队列 +
确定性重试/死信**导出到一个本地模拟上报端。目标是让高并发写入、采样决策与
导出失败链路在每个入口下都**确定、可解释、可外部查询**。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 统一链路模型（`RawRecord`/`Span`/ID），以及归一化、采样、失败三类枚举 |
| `internal/ingest` | 多来源上下文归一化、字段校验、去重/冲突与断裂归类 |
| `internal/sampler` | 父级一致、按 trace 冻结、对 traceID 确定性派生的采样器 |
| `internal/export` | 本地模拟上报端 `MockReporter` + 有界导出器（背压/重试/死信/统计） |
| `internal/retention` | 运行期状态的有界保留：容量+TTL、确定性回收、有界回收日志（重访归类） |
| `internal/pipeline` | 串联 `归一化 → 采样 → 导出` 的并发安全入口 |
| `cmd/demo` | 本地可运行演示 |

数据流：

```
 RawRecord ──▶ Normalizer ──(model.Outcome)──▶ Sampler ──(Decision)──▶ 有界 Exporter ──▶ MockReporter
 (3 种来源)     校验/归类/去重                  按 trace 冻结            单 worker 批次重试/死信
```

## 统一链路模型

- 标识采用 W3C 风格：`TraceID` 为 16 字节（32 hex），`SpanID` 为 8 字节（16 hex），
  全 0 与非法十六进制均在入口被拒绝。
- `model.RawRecord` 保留来源原始字符串（`TraceID/SpanID/ParentSpanID/ParentTraceID`），
  因此非法输入可以在**入口**被精确归类，而不是在深层 panic 或被静默丢弃。
- 归一化成功后得到 `model.Span{TraceID, SpanID, Parent, HasParent, ...}`。

来源由 `model.Source` 区分：`http_headers`、`binary_v2`、`inproc`；它们只是字段
承载形式不同，归一化后进入同一模型，彼此不共享可变状态。

- `http_headers` / `inproc`：id 以十六进制字符串承载（`RawRecord.TraceID/SpanID/...`）。
- `binary_v2`：id 以数值承载（`RawRecord.TraceNumHi/TraceNumLo/SpanNum/ParentSpanNum`），
  0 表示缺失；数值被大端编入定长 `TraceID/SpanID`。

### 归一化归类（确定且可区分，不静默丢弃、不相互污染）

`model.Class` 取值（见每条记录返回的 `model.Outcome{Status, Reason, ...}`）：

| Class | 触发条件 |
| --- | --- |
| `ok` | 归一化为合法 `Span` |
| `missing_context` | trace id 或 span id 缺失/空白 |
| `invalid_field` | 来源未知、id 长度/字符非法或全 0、时间为负或 end<start |
| `broken_parent` | 父 span id 非法、自引用、父 trace 存在却跨链路、或只给父 trace 不给父 span |
| `conflict` | 同一 `SpanID` 已注册在**另一条** trace 下 |
| `duplicate` | 同一 `(TraceID,SpanID)` 已在本 trace 注册 |

要点：

- 归类与到达顺序无关。父跨度**尚未到达**不会被判为断裂（它可能稍后到达）；
  `broken_parent` 仅表示结构上确定无法成立的关系。
- 重复/冲突只返回归类结果，绝不覆盖、改动已接受的数据，因此无法污染其他记录。
- 各 Class 计数可通过 `Normalizer.ClassCounts()` / `Pipeline.Stats().ByClass` 查询。

## 采样规则（父级一致、按链路冻结、全有或全无）

- 决策以 `TraceID` 为 key：同一 trace 的所有跨度查同一条缓存结论，因此父子结论
  天然一致，且与父子到达顺序无关。
- 某 trace 第一次出现时：若已知父跨度结论则继承；否则用
  `FNV-1a(TraceID) mod 10000 < ratio*10000` 做确定性派生。固定比例下，同一条
  trace 在任意进程/任意时刻结论相同。
- 结论一旦产生即**冻结**：运行中调用 `SetSamplingRatio` 只影响尚未开始的 trace，
  不能翻转已开始链路的结论。
- 未被采样的链路**不会只产出部分跨度**：drop 决策下该 trace 的每个跨度都在采样处
  被跳过，导出端收到的每条 trace 跨度数要么是 0、要么是完整集合。
- 比例裁剪到 `[0,1]`，`NaN/Inf` 视为 0；计数器（已决 trace、采样、丢弃）通过
  `Sampler.Snapshot()` / `Pipeline.Stats()` 查询。

## 有界导出、背压与失败处理

`export.Exporter` 是单 worker、批量、有界的导出阶段：

- **有界内存**：内部是容量 `QueueCapacity` 的 channel 加至多一个在途批次；去重/
  死信账簿同样有界（`pending ≤ Queue+BatchMax`、死信环 `DeadLetterCapacity`、
  近期已送达环 `4*QueueCapacity`），内存不会随唯一跨度数量无界增长。
- **生产者不被无限阻塞**：`Enqueue` 先走非阻塞发送，再做一次很短（`enqueueWait`）
  的有界等待；仍满则立即返回 `FailQueueFull`，绝不无限挂起。
- **不重复报送（有界精确一次）**：入队时按生命周期登记 SpanKey——`pending`（排队/
  在途）、`dead`（死信环中）、`recent`（近期成功送达的 FIFO 幂等尾）。同一跨度在
  保护期内再次入队返回 `FailDuplicate`，上报端对同一跨度最多收到一次。
- **重试**：单 worker 对每个批次最多重试 `MaxAttempts` 次，退避 `RetryBackoff`，
  单次调用有 `attemptTimeout` 上限；卡住的端点不会永久占用 worker。
- **死信**：重试耗尽后，批次内每个跨度**恰好一次**进入有界死信环；环满时确定性地
  丢弃**最旧**条目并计入 `DeadLetterDrops`。死信可经 `DeadLetters()` 外部查看，
  且死信跨度不会被再次受理报送。
- **显式 Flush**：`Flush(ctx)` 由单 worker 同步排空并回执，测试可确定性等待，
  不依赖 sleep 轮询。

`export.MockReporter` 是本地脚本化上报端，可用 `Behavior` 原子切换：

- `ModeOK`：全部接受；
- `ModeSlow`：每次先 sleep `Delay`，可被 ctx 取消并归类为 `endpoint_timeout`；
- `ModeFail`：按 `Failure` 失败；`FailModulo>0` 时每 N 次（从 1 起）确定性失败；
- `InFlight>0`：模拟在途上限，拿不到容量快速失败为 `endpoint_rejected`。

`FailureClass`：`endpoint_timeout / endpoint_rejected / endpoint_error /
queue_full / attempts_exhausted / duplicate`，全部计数可查询
（`Reporter.FailureCounts()`、`Exporter.Stats()`）。

> 背压被拒的数据如何补偿由调用方决定：可记录后调用 `Pipeline.EnqueueSpan`
> 复用**已归一化、已冻结决策**的跨度做有界重试（不要重发原始记录，否则会被正确
> 判为 `duplicate`），或按业务丢弃并统计 `queue_full`。

## 运行期状态保留与回收（长期驻留的硬上界）

进程常驻运行、持续摄入大量唯一链路时，入口侧需要保留的状态（归一化的身份注册表、
冻结的采样结论、父跨度结论）全部由 `internal/retention` 的有界存储承载，**默认配置
即有硬上界**，老代码不改调用方式（`pipeline.New` / `ingest.New` / `sampler.New`）
自动获得该上界。

### 配置

```go
rcfg := retention.Config{
    MaxEntries:      32768,          // 活条目硬上限（容量维度）
    TTL:             10 * time.Minute, // 触达后最长保留窗口（时间维度，<=0 关闭）
    JournalCapacity: 32768,          // 回收日志（tombstone）上限，用于重访归类
}
p := pipeline.NewWithRetention(exportCfg, 0.5, rcfg, logf) // pipeline.New 等价于用 DefaultConfig()
```

`MaxEntries` 与 `TTL` 是**组合**生效的：活条目数永远 `<= MaxEntries`；条目在最后一次
触达后超过 `TTL` 即被回收。每次读写都会刷新该条目的触达时间（LRU 语义），因此持续
有跨度到达的链路不会被回收，只有"不再回来"的老链路会被确定性回收。

### 状态上界怎么算

| 状态 | 持有者 | 上界 |
| --- | --- | --- |
| 跨度身份注册表 `spanOwner` | `ingest.Normalizer` | `MaxEntries` |
| 冻结采样结论 `decisions` | `sampler.Sampler` | `MaxEntries` |
| 父跨度结论 `spanVerdict` | `sampler.Sampler` | `MaxEntries` |
| 回收日志（每 store 一份） | 上述三处 | 各 `JournalCapacity` |
| 导出防重复簿记 `pending/dead/recent` | `export.Exporter` | `QueueCapacity+BatchMax`、`DeadLetterCapacity`、`4*QueueCapacity`（原有，不变） |

进程保留的链路状态总量因此是配置常数的函数，与"见过的唯一链路数"无关。

### 回收规则（确定、可复现、不卡住入口）

- 回收顺序确定：永远先回收**最久未触达**的条目；同一事件序列必然得到同一保留集合。
- 写路径每次回收的工作量有界（过期扫描分批 `sweepBatch=256`，容量回收按新条目数
  逐个进行，均为摊还 O(1)），入口不会被长时间卡住；完整扫描只发生在查询路径上。
- 每次回收都计入 `EvictedTotal`（含 `ExpiredTotal` 细分）并可经日志钩子输出
  （`retention evict stage=... trace=... reason=capacity|expired`）；当前实际保留量
  `Retained`、容量 `Capacity`、回收次数均可通过 `Pipeline.Stats().Retention`、
  `Sampler.Snapshot()`、`Normalizer.RetentionStats()` 查询。

### 回收后重访的语义

- **保留期内**：一切既有语义不变——父子采样结论一致、运行中改比例不翻转已开始
  链路、未采样链路不会只导出部分跨度、重复/冲突归类不变。
- **保留期结束后同一条链路再次出现**（迟到的子跨度、重复上报）：确定性地进入一个
  **新生命周期**，并给出外部可识别的归类——`model.Outcome.Revisit` /
  `pipeline.Result.Revisit` 为 `true`，日志带 `lifecycle=revisit`，计数经
  `Stats().Retention.Revisits` 查询。绝不静默丢弃，也绝不并入其他链路的结论。
- 若该链路仍**有父跨度结论在保留期内**，新到达的跨度沿用这条仍受保护的原有结论
  （与既有"父级一致"规则同源）；否则按 trace id 与**当前**比例重新派生——这与
  "改比例不翻转已开始链路"不矛盾：被回收的链路已结束其生命周期。
- 重访识别依赖有界回收日志：被回收的标识在 `JournalCapacity` 窗口内再次出现时
  归类为 `revisit`；窗口之外则在信息论上无法与"全新链路"区分，按全新链路处理——
  行为仍然确定，只是归类不同。journal 本身也是有界的，不会成为新的增长点。

### 与采样 / 背压 / 死信的关系

- 回收只作用于**入口侧**状态；导出侧的防重复簿记（`pending/dead/recent`）独立保持
  有界，其当前量与上界经 `Exporter.Stats()`（`PendingLen/RecentLen/RecentCap/
  DeadLetterLen`）查询。
- 一个跨度**已受理未上报完、在死信环中、或仍在 `recent` 幂等尾内**时，即使入口侧
  状态已被回收并以 `revisit` 重新进入，导出器仍返回 `FailDuplicate` 拒绝重复入队——
  保护范围内不允许重新开始生命周期。
- 只有当该跨度滚出 `recent` 保护范围后，同一 `(TraceID,SpanID)` 才允许以新生命周期
  被再次受理、再次上报；这次重报同样带 `revisit` 归类，可外部区分。

## 本地验证

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单元测试，含竞态检测（高并发一致性 / 不重不漏 / 故障 / 背压）
go test -race ./...

# 仅看关键集成用例（-v 会打印链路标识、采样结论、失败归类日志）
go test -race -v ./internal/pipeline

# 回收与重访相关用例
go test -race -v ./internal/retention/ -run .
go test -race -v ./internal/pipeline/ -run 'Eviction|Revisit|Retention'

# 本地端到端演示：三来源归一化、拒绝归类、运行中改比例、端点故障、慢端点背压、
# 保留回收与回收后重访
go run ./cmd/demo
```

### 测试覆盖

- `internal/model`：ID 解析/渲染/零值/非法输入。
- `internal/ingest`：三种来源归一化；六类归类；重复与冲突不污染；父跨度晚到不
  误判；并发下合法记录互不干扰。
- `internal/sampler`：父子一致与乱序到达；中途改比例不翻转；固定比例确定性；
  0/1 边界全有或全无；同一 trace 并发决策一致。
- `internal/export`：精确一次送达与重复拒绝；卡死端点下有界 `queue_full`；
  重试耗尽入死信恰好一次；死信环有界丢最旧；死信幂等；`MockReporter` 各模式。
- `internal/retention`：容量回收最冷优先、TTL 过期（注入假时钟）、回收日志有界、
  并发写下持续回收时硬上界恒成立、默认配置即有界。
- `internal/pipeline`：
  - `TestConcurrentNoDuplicateNoLoss`：1000 个并发跨度，送达集合不重不漏；
  - `TestConcurrentSamplingAllOrNothing`：每条 trace 导出跨度为 0 或完整集合，
    且与采样器冻结结论、采样/丢弃计数完全吻合；
  - `TestEndpointFailureDeadLettersAndCounters`：故障期跨度恰好一次进死信、计数可查；
  - `TestBackpressureUnderStuckEndpoint`：慢端点下生产者快速得到 `queue_full`、
    不被无限阻塞，已受理跨度最终不丢不重；
  - `TestConcurrentWritesWithContinuousEviction`：3000 条唯一链路并发写入、回收
    持续进行，保留量始终不超容量、回收次数可查，已受理跨度仍不重不漏；
  - `TestRevisitAfterEviction`：链路状态被回收后重访——导出保护期内重提交被判
    `duplicate` 不重复报送，迟到子跨度以 `revisit` 新生命周期采样导出，滚出防
    重复保护范围后同一跨度才允许再次受理；
  - `TestDefaultRetentionIsBounded`：默认配置下各状态上界存在且可查询。

单测日志由 `testing.T.Log` 输出，关键行包含链路标识与结论，例如：

```
trace=... span=... decision=sample export=enqueued
trace=... span=... decision=drop class=ok
trace=... span=... decision=sample export=rejected failure=queue_full
trace=... span=... decision=sample export=enqueued lifecycle=revisit
ingest reject source=inproc hint=inproc:<trace>/<span> class=broken_parent reason="..."
export attempt=2/3 failed class=endpoint_error spans=8 trace=...
retention evict stage=sampler-decisions trace=... decision=sample reason=capacity
retention evict stage=ingest-span-owner trace=... span=... reason=expired
```
