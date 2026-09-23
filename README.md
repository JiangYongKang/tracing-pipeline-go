# tracing-pipeline-go

进程内的追踪采集与导出管道（in-process tracing pipeline）。它把**多种本地模拟上报端**
产生的跨度归一为统一链路模型，做**基于父级的采样决策**，并通过**有界队列 + 重试 +
有界死信队列（DLQ）**把数据送给一个**本地模拟的接收端（MockReporter）**。所有关键行为
——归一化归类、采样结论、背压、失败处置——都是确定且可解释、可被外部查询的。

无任何第三方依赖，仅用标准库；Go >= 1.25。

## 1. 包结构

```
pkg/model/      统一链路模型（TraceID/SpanID/Span/Kind/Status）与严格的 ID 解析
pkg/normalize/  三种来源格式的归一化 + 四类拒绝归类（不静默丢弃、不相互污染）
pkg/sampling/   基于父级的一致性采样（确定哈希 + 有界决策缓存）
pkg/export/     有界队列、背压入口、重试退避、有界 DLQ、模拟上报端
pkg/metrics/    全部计数器/量表（原子操作，随时可外部查询）
pkg/pipeline/   门面：把四个阶段串成并发安全的一条管道
```

## 2. 统一链路模型（pkg/model）

| 字段 | 说明 |
| --- | --- |
| `TraceID` | 16 字节（W3C 形态，32 hex）；全 0 / 长度不对 / 非 hex 均非法 |
| `SpanID`  | 8 字节（16 hex）；全 0 / 长度不对 / 非 hex 均非法 |
| `ParentID` | 8 字节；零值 = 根跨度 |
| `Kind` | `unspecified/internal/server/client/producer/consumer` |
| `Start/End` | UTC 时间；要求 `start>0` 且 `end>=start` |
| `StatusCode` | `unset/ok/error` |
| `Attributes` | 入参 map 会被拷贝，来源之间不共享底层数据，杜绝相互污染 |
| `Source` | 记录来源标签（`http-json-v1`/`w3c-context-v1`/`zipkin-json-v1`），仅随记录走 |

支持的三种本地来源：

- **HTTP JSON v1**（`HTTPSpanV1`）：hex 字符串 id、纳秒时间戳、字符串 kind/status。
- **W3C trace-context v1**（`W3CSpanV1`）：`vv-traceid-spanid-flags` 的 `traceparent`，
  纳秒 `ts`/`dur`，数值 kind，`*bool ok` 表示成功与否。
- **Zipkin JSON v1**（`ZipkinSpanV1`）：兼容 16 hex（64 位）与 32 hex（128 位）traceId，
  微秒时间戳，大写 `SERVER/CLIENT` kind，`Error` 字段映射为错误状态。

## 3. 归一化与拒绝归类（pkg/normalize）

**批次契约**：同一条 trace 的所有跨度必须在同一个批次里提交。断裂父子关系是在
**批次内确定性检测**的，跨批次 trace 不属于支持范围（刻意的设计边界，行为可预测）。

每条输入记录最终**恰好出现一次**：要么进 `Result.Accepted`，要么进 `Result.Rejected`
（按输入下标排序），永远不会被静默丢弃。四类可区分的归类：

| 类别 | 触发条件 |
| --- | --- |
| `missing_context` | trace/span id 缺失、非法、全 0（含 W3C `traceparent` 里的非法 id） |
| `invalid_fields`  | id 合法但其它字段无法解析：未知 kind/status、时间戳非法、`traceparent` 结构错、版本不支持等 |
| `broken_link`     | 父 id 语法合法但在本 trace 内解析不到：父缺失、**跨 trace 引用**、**父子成环**、或父记录自身非法/重复 |
| `duplicate_span`  | 同一 `(trace_id, span_id)` 出现多次（首个保留，后续归为此类） |

关键不变量：**坏记录不会污染好记录**。断裂父节点的整棵分离子树都会被显式归类为
`broken_link`，而不会被当作“伪根”悄悄混进健康链路。

## 4. 基于父级的采样规则（pkg/sampling）

1. **根跨度定调**：根跨度首次出现时建立决策，决策由
   `FNV-1a(trace_id)/2^64 < ratio` 确定性得出。只依赖 trace id，与到达顺序、并发无关。
2. **子承父决**：任何子跨度都继承其 trace 已记录的决策；先到的是孙子也会替整链定调，
   同链所有跨度结论必然一致。
3. **运行中改比例不翻转**：`SetRatio` 只影响“根尚未出现”的新链路；已经开始的链路决策
   被缓存钉住，永不重算、永不翻转（测试 `TestRatioChangeNeverFlipsStartedTrace`）。
4. **未采样整链丢弃**：管道以**整条 trace** 为单位调用 `DecideBatch` 并按链过滤，
   DROP 的链路一个跨度都不会入队，因此不可能只产出部分跨度。
5. **有界缓存**：决策缓存容量 `CacheCap`（默认 65536），FIFO 淘汰；导出完成的 trace 会
   `ReleaseTrace` 及时释放。容量配置应远大于并发链路数；极端超载下 FIFO 淘汰是
   文档化的确定性兜底，而不是内存无界增长。
6. `ratio=0` 全丢、`ratio=1` 全留；越界/NaN 比例被夹到 `[0,1]`。

## 5. 有界导出、背压与失败处理（pkg/export）

### 5.1 背压（生产者不被无限阻塞、内存不无限增长）

- 导出队列是**容量固定的 buffered channel**（`QueueCap`），管道内缓存的批次/跨度数量
  因此有硬上界。
- `OfferBatch`：**非阻塞**，队列满立即返回 `false`（管道层据此回传 `QueueFullTraces`）。
- `SubmitBatch`：可阻塞入队，但**可被 context 中断**，不存在无限阻塞。
- 上报端变慢时，队列打满后入口立刻显式背压；上报端恢复后，所有已准入数据
  **恰好一次**送达（测试 `TestReporterSlowBackpressure`）。

### 5.2 失败链路（确定性，不重复报送）

- **投递单位是整条 trace**（`SpanBatch`），重试是“整批原子”的：一批要么成功一次，
  要么一次都不成功，不存在部分送达，因此不会产生重复的部分报送。
- 失败后按**固定退避**重试，最多 `MaxAttempts` 次（含首次）。
- 重试用尽 → 进入**有界 DLQ**（容量 `DLQCap`）。
- **DLQ 满时的确定性策略：丢最旧（drop-oldest）**——牺牲 DLQ 里最老的批次并计数
  （`dropped_dlq_full`、`dlq_depth`），保留较新的失败批次；任何丢弃都不静默。
- 只有显式调用 `DrainDLQ` 才会重投 DLQ；**成功即移除**，反复 drain 不会重复报送
  （at-most-once）。
- 同一 trace id 的重复提交会在入口被抑制并计数 `duplicates`（去重窗口 65536，FIFO）。
- `Close`：停止接收入口（之后提交返回 `ErrClosed`），等 worker 把已缓存数据排空后停止；
  DLQ 中保留的批次**不报送、可继续检视**。

### 5.3 本地模拟上报端 MockReporter

`MockReporter` 用开关和规则确定性地模拟故障，便于复现：

- `Available bool`：`false` 时所有投递立即归类为 `unavailable`（可运行时翻转，模拟宕机/恢复）。
- `Latency`：每次投递耗时（可运行时调整，模拟变慢导致背压；等待期间可被 context 打断）。
- `Rule func(SpanBatch) FailureClass`：按批次确定性产出 `timeout` / `unavailable` /
  `rejected`，空结论表示成功。
- 只在**成功**时记录 `trace id -> 已接收跨度数` 与成功批次计数，因此可以直接做
  “不重不漏”的精确集合断言。

## 6. 可查询的指标（pkg/metrics）

全部为原子 int64，任意时刻都能 `Counters().Snapshot()` 读取，无需停管道：

`ingested, accepted, rejected, sampled_in, sampled_out, export_attempts,
export_successes, export_failures, retried, dlq_enqueued, dlq_drained,
dropped_dlq_full, duplicates, queue_depth, dlq_depth`。

恒等关系（测试中对账）：

- `ingested == accepted + rejected`
- `accepted == sampled_in + sampled_out`
- 正常期 `export_successes == 已准入且采样保留的链路数`（慢/宕机场景另由
  `dlq_*`、`dropped_dlq_full` 解释差异）。

## 7. 使用示例

```go
rep := export.NewMockReporter(export.MockReporterConfig{Available: true})
p := pipeline.New(pipeline.Config{
    Sampling: sampling.Config{Ratio: 0.5, CacheCap: 10000},
    Export:   export.Config{QueueCap: 128, DLQCap: 64, MaxAttempts: 3,
                            Backoff: 5 * time.Millisecond, DeliveryTimeout: time.Second},
}, rep)

ctx, cancel := context.WithCancel(context.Background())
p.Start(ctx)

res := p.IngestHTTP([]normalize.HTTPSpanV1{ /* 一整条 trace 的跨度 */ })
res.Decisions            // 每个 trace 的 KEEP/DROP
res.EnqueuedTraces       // 采样保留且成功入队的 trace
res.QueueFullTraces      // 因背压未入队的 trace
res.Normalized.Rejected  // 四类拒绝归类 + 明细

// 上报端恢复后重投 DLQ：
redelivered, remaining := p.Exporter().DrainDLQ(context.Background())
snap := p.Counters().Snapshot()  // 外部查询全部计数/状态

cancel()
_ = p.Shutdown(context.Background())
```

另有 `IngestW3C`、`IngestZipkin` 两个等价入口；三个入口可并发混用。

每个来源还提供**可阻塞但可中断**的入口 `SubmitHTTP / SubmitW3C /
SubmitZipkin(ctx, batch)`：队列满时等待容量或 ctx 结束，适用于“不允许因背压丢数据”
的生产者（ctx 保证不会无限阻塞）；遇到重复 trace 返回 `export.ErrDuplicate`（幂等，
已计入 `duplicates`），pipeline 层将其视为正常情况不报错。非阻塞入口遇到队列满则在
`IngestResult.QueueFullTraces` 中显式返回，由调用方决定丢弃或重试。

## 8. 本地验证方法

```bash
# 全量单测（含竞态检测）
go test ./... -race -count=1

# 覆盖率
go test ./... -cover

# 静态检查与格式
go vet ./...
gofmt -l pkg/

# 只跑故障恢复链路 / 并发一致性场景
go test ./pkg/pipeline/ -run TestReporterOutageThenRecoveryWalksTheFailureChain -v
go test ./pkg/pipeline/ -run TestConcurrentSamplingConsistencyAndNoLossNoDup -v
```

测试日志会打印每条链路的**标识、采样结论与失败归类**，例如：

```
trace=... decision=KEEP spans=5 action=enqueued ratio=0.5000
trace=... decision=DROP spans=21 whole-trace drop, no partial spans
trace=... class=broken_link reject_index=5 detail="... references parent ... absent ..."
trace=... failure_class=unavailable attempts=12 failures=6 dlq=3 dropped_oldest=3
trace=... failure_class=unavailable -> recovered, redelivered=once spans=2
CONCURRENCY: goroutines=24 traces=960 spans=4800 delivered=960 unique=960 duplicate_batches=0
```

### 测试矩阵（关键场景）

| 测试 | 验证内容 |
| --- | --- |
| `normalize.TestRejectClasses` 等 | 四类拒绝归类、坏记录不污染好记录、断裂子树、跨 trace 引用、三来源解析、总数对账 |
| `sampling.TestParentChildConsistency` | 父子/乱序到达下结论一致 |
| `sampling.TestRatioChangeNeverFlipsStartedTrace` | 运行中改比例不翻转已开始链路；新链路遵循新比例 |
| `sampling.TestNoPartialSpansOnDrop` | 未采样链路整链丢弃，不产出部分跨度 |
| `sampling.TestConcurrentDecisionsSameTrace` | 200 goroutine 下同一 trace 结论一致 |
| `export.TestOfferBoundedQueueBackpressure` / `...Interruptibly` | 有界背压、非阻塞/可中断提交、内存上界 |
| `export.TestRetryThenDLQ` / `TestDLQOverflowDropsOldest` | 重试→DLQ、满则丢最旧且计数 |
| `export.TestDrainDLQAtMostOnce` | DLQ 重投恰好一次，不重复 |
| `export.TestReporterSlowBackpressure` | 慢上报端背压 + 恢复后已准入数据不重不漏 |
| `pipeline.TestConcurrentSamplingConsistencyAndNoLossNoDup` | 24×40 条链路经三个入口并发写入：采样一致、不重不漏、跨度数精确 |
| `pipeline.TestReporterOutageThenRecoveryWalksTheFailureChain` | 宕机→重试耗尽→DLQ→丢最旧→恢复→恰好一次重投，全计数对账 |
| `pipeline.TestBackpressureVisibleAtPipelineEntry` | 队列满在入口显式可见（`QueueFullTraces`）而非无限阻塞 |
