# tracing-pipeline-go

进程内的追踪采集与导出管道（Go，无第三方依赖）。它把来自不同来源的追踪上下文
归一为统一链路模型，做**基于父级、按链路生命周期冻结**的采样，并通过**有界队列 +
确定性重试/死信**导出到一个本地模拟上报端；入口侧需要保留的运行期链路状态由
**有界生命周期注册表（容量 + TTL，LRU 回收，墓碑重访归类）**治理，进程可常驻运行而
状态不随唯一链路数无界增长。目标是让高并发写入、采样决策、导出失败链路与回收/重访
在每个入口下都**确定、可解释、可外部查询**。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 统一链路模型（`RawRecord`/`Span`/ID），以及归一化、采样、失败三类枚举 |
| `internal/ingest` | 多来源上下文归一化、字段校验、去重/冲突与断裂归类 |
| `internal/sampler` | 父级一致、按 trace 生命周期冻结、对 traceID 确定性派生的采样器 |
| `internal/retention` | 运行期链路状态的有界生命周期注册表（容量+TTL、LRU 回收、重访归类） |
| `internal/export` | 本地模拟上报端 `MockReporter` + 有界导出器（背压/重试/死信/统计） |
| `internal/pipeline` | 串联 `归一化 → 采样 → 导出` 的并发安全入口 |
| `cmd/demo` | 本地可运行演示 |

数据流：

```
 RawRecord ──▶ Normalizer ──(model.Outcome)──▶ Sampler ──(Decision)──▶ 有界 Exporter ──▶ MockReporter
 (3 种来源)     校验/归类/去重                  按生命周期冻结           单 worker 批次重试/死信
                    └──────────── 共享 retention.Registry（有界、按链路生命周期回收） ────────────┘
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
| `conflict` | 同一 `SpanID` 已注册在**另一条**仍存活的 trace 下 |
| `duplicate` | 同一 `(TraceID,SpanID)` 已在当前生命周期注册（含“保护性重访”，见下文） |
| `revisit` | 记录合法，但其 trace 的上一个运行期生命周期已被回收；本条记录开启一个**新生命周期**（generation ≥ 2），与 `ok` 一样进入采样/导出 |

要点：

- 归类与到达顺序无关。父跨度**尚未到达**不会被判为断裂（它可能稍后到达）；
  `broken_parent` 仅表示结构上确定无法成立的关系。
- 重复/冲突只返回归类结果，绝不覆盖、改动已接受的数据，因此无法污染其他记录。
- 各 Class 计数可通过 `Normalizer.ClassCounts()` / `Pipeline.Stats().ByClass` 查询。
- `model.Outcome` 额外携带 `Revisit` / `Protected` / `Generation` 三个字段，
  让“回收后重访”的两种形态（新生命周期 / 沿用仍受保护的旧结论）对外可区分。

## 采样规则（父级一致、按生命周期冻结、全有或全无）

- 决策以 `TraceID` 为 key：同一 trace 同一生命周期内的所有跨度查同一条冻结结论，
  因此父子结论天然一致，且与父子到达顺序无关。
- 某 trace 第一次出现时：用 `FNV-1a(TraceID) mod 10000 < ratio*10000` 做确定性派生。
  固定比例下，同一条 trace 在任意进程/任意时刻结论相同。
- 结论在**生命周期内冻结**：运行中调用 `SetSamplingRatio` 只影响尚未开始的
  生命周期，不能翻转仍存活的已开始链路的结论。
- 未被采样的链路**不会只产出部分跨度**：drop 决策下该 trace 的每个跨度都在采样处
  被跳过，导出端收到的每条 trace 跨度数要么是 0、要么是完整集合。
- 比例裁剪到 `[0,1]`，`NaN/Inf` 视为 0；计数器（存活 trace、采样、丢弃）通过
  `Sampler.Snapshot()` / `Pipeline.Stats()` 查询。

## 运行期状态的生命周期与容量治理（retention）

进程常驻运行时会持续见到大量**唯一链路**。入口侧的重复/冲突判定（span→trace 登记）
和采样冻结结论都需要按 trace 保留状态；这些状态现在统一由
`internal/retention.Registry` 持有——normalizer 与 sampler **共享同一个注册表**，
一条 trace 的“去重登记 + 冻结采样结论”构成一个**生命周期（lifecycle）**，同生同灭，
回收语义因此对两个阶段完全一致。

### 配置（`retention.Config`，零值即有界）

| 字段 | 含义 | 默认值 |
| --- | --- | --- |
| `MaxTraces` | 同时存活的链路生命周期硬上限 | 10 000 |
| `TTL` | 空闲（无新跨度/新决策）多久后可被回收 | 10 分钟 |
| `TombstoneCap` | 回收后保留多少“墓碑”用于重访归类 | 10 000 |
| `SweepBatch` | 单次写入最多摊还多少条 TTL 回收 | 8 |
| `Now` | 时钟注入（测试用），nil 用真实时钟 | `time.Now` |

`pipeline.New(...)` 使用上述默认值（老代码调用方式不变）；
`pipeline.NewWithRetention(cfg, ratio, retCfg, logf)` 可显式配置。

### 状态上界怎么算

- 存活生命周期 ≤ `MaxTraces`；每个生命周期持有该 trace 的冻结结论与其 span 登记集合，
  因此入口侧状态只随**存活**链路数增长，绝不随**历史见过的唯一链路数**增长。
- 墓碑 ≤ `TombstoneCap`（FIFO 淘汰）。墓碑只记 `(TraceID, generation)`，不持有跨度数据。
- 不变量恒成立：`Lifecycles − EvictedCapacity − EvictedTTL == LiveTraces`（单测断言）。
- 导出侧簿记保持原有上界：`pending ≤ QueueCapacity+BatchMax`、`dead ≤ DeadLetterCapacity`、
  `recent ≤ 4*QueueCapacity`，与 retention 相互独立，均不引入新的无界增长点。

### 回收规则（确定、可复现、不卡写入）

- **容量回收**：达到 `MaxTraces` 时逐出 LRU 尾部（最久未触达）生命周期，每次插入最多
  逐出一个，工作量是 O（被逐链路的跨度数），写入入口不会被长时间卡住。
- **TTL 回收**：每次写入摊还检查 LRU 尾部至多 `SweepBatch` 条，空闲超过 `TTL` 即回收；
  列表按触达时间有序，遇到第一条未过期即停止。无后台 goroutine，行为完全可复现
  （测试用注入时钟确定性验证）。
- 每次回收都留下一条墓碑并计数；`Retention.Stats()`（经 `Pipeline.Stats().Retention`）
  对外暴露 `LiveTraces / LiveSpans / Tombstones / EvictedCapacity / EvictedTTL /
  Revisits / Lifecycles`。

### 回收后重访的语义（确定且可区分，绝不静默丢弃）

同一条 trace 在保留期结束后再次出现（迟到的子跨度、重复上报）时，按 trace id 查墓碑：

1. **仍在导出防重复保护窗口内**（该跨度仍处在导出侧的 `pending`/`dead`/`recent`
   簿记中，`Exporter.IsProtected` 为真）：**不开启新生命周期、不重新登记、不重新提交**，
   沿用仍受保护的原结论。归类为 `duplicate` 且 `Outcome.Revisit=true, Protected=true`，
   计入 `Pipeline.Stats().ProtectedRevisits`，日志打 `ingest revisit=protected ...`。
2. **已超出保护范围**：开启一个**新生命周期**（generation = 墓碑代际 + 1），归类为
   `ClassRevisit`（`revisit`），重新做确定性采样派生并正常进入导出；计入
   `Pipeline.Stats().Revisits`，日志打 `ingest revisit=new-lifecycle trace=... gen=N`。
   新生命周期与原生命周期的结论互不串扰：决策按生命周期冻结，导出防重复按 SpanKey。
3. 墓碑本身也被 FIFO 淘汰后，再次出现与全新链路不可区分（按 gen=1 处理）——
   这是墓碑窗口之外的确定行为，可通过调大 `TombstoneCap` 控制窗口大小。

### 与采样/背压/死信的关系

- **采样**：保留期内结论冻结、父子一致、中途改比例不翻转——这些语义不变；回收后
  重访属于新生命周期，按当前比例重新派生（固定比例下派生结果与之前相同）。
- **背压/重试**：回收只清理入口侧判定状态，不动导出队列；`queue_full` 背压与
  `EnqueueSpan` 补偿重试行为不变。
- **死信/防重复**：死信环与 `recent` 幂等尾仍是有界的“保护窗口”；处于窗口内的跨度
  在回收时**不会**被当成可重新提交对象（见上第 1 条），只有超出窗口后才允许新生命周期
  再次导出同一跨度——此时端点第二次收到它属于**新生命周期的确定行为**，而非簿记泄漏。
- 新状态全部经既有入口查询：`Pipeline.Stats().Retention / .Revisits /
  .ProtectedRevisits`、`Exporter.Stats().PendingLen/DeadSetLen/RecentLen/RecentCap`。

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
  `Exporter.IsProtected(key)` 把这个保护窗口暴露给 retention：回收后重访的跨度
  若仍在窗口内，不会被当成可重新提交对象（见“回收后重访的语义”）。
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

## 本地验证

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单元测试，含竞态检测（高并发一致性 / 不重不漏 / 故障 / 背压 / 回收与重访）
go test -race ./...

# 仅看关键集成用例（-v 会打印链路标识、采样结论、回收与重访归类日志）
go test -race -v ./internal/pipeline

# 本地端到端演示：三来源归一化、拒绝归类、运行中改比例、端点故障、慢端点背压、
# 保留回收与回收后重访
go run ./cmd/demo
```

### 测试覆盖

- `internal/model`：ID 解析/渲染/零值/非法输入。
- `internal/ingest`：三种来源归一化；七类归类；重复与冲突不污染；父跨度晚到不
  误判；并发下合法记录互不干扰。
- `internal/sampler`：父子一致与乱序到达；中途改比例不翻转；固定比例确定性；
  0/1 边界全有或全无；同一 trace 并发决策一致。
- `internal/retention`：容量 LRU 回收的确定性；注入时钟下的 TTL 摊还回收；重访
  开启新代际；保护性重访不改任何状态；生命周期内重复/冲突；决策按生命周期冻结；
  小容量下并发 Register/Decide 的不变量（存活 ≤ 上限、生命周期账本恒等）。
- `internal/export`：精确一次送达与重复拒绝；卡死端点下有界 `queue_full`；
  重试耗尽入死信恰好一次；死信环有界丢最旧；死信幂等；`MockReporter` 各模式。
- `internal/pipeline`：
  - `TestConcurrentNoDuplicateNoLoss`：1000 个并发跨度，送达集合不重不漏；
  - `TestConcurrentSamplingAllOrNothing`：每条 trace 导出跨度为 0 或完整集合，
    且与采样器冻结结论、采样/丢弃计数完全吻合；
  - `TestEndpointFailureDeadLettersAndCounters`：故障期跨度恰好一次进死信、计数可查；
  - `TestBackpressureUnderStuckEndpoint`：慢端点下生产者快速得到 `queue_full`、
    不被无限阻塞，已受理跨度最终不丢不重；
  - `TestConcurrentWritesWithContinuousReclaim`：16 路并发写入 4000 条唯一链路、
    容量上限仅 64 的持续回收下，存活状态始终 ≤ 上限、账本恒等、已受理跨度仍不重不漏；
  - `TestRevisitAfterReclaim`：回收后重访的两阶段语义——保护窗口内沿用旧结论
    （不重投、计数可查），窗口外开启 generation=2 新生命周期并可再次导出。

单测日志由 `testing.T.Log` 输出，关键行包含链路标识、结论与回收/重访归类，例如：

```
trace=... span=... gen=1 decision=sample export=enqueued
trace=... span=... gen=1 decision=drop class=ok
trace=... span=... gen=1 decision=sample export=rejected failure=queue_full
ingest reject source=inproc hint=inproc:<trace>/<span> class=broken_parent reason="..."
ingest revisit=protected source=inproc hint=... class=duplicate reason="trace reclaimed but span still under export duplicate protection; original conclusion stands"
ingest revisit=new-lifecycle trace=... span=... gen=2
export attempt=2/3 failed class=endpoint_error spans=8 trace=...
```
