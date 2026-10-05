# chain-reorg-indexer-go

面向区块链节点的**增量索引组件**。链数据有两种接入方式，索引行为与结果完全一致：

1. **进程内**：直接注入任何实现 `rpc.Client` 的对象（如可编程假节点）；
2. **HTTP JSON-RPC**：通过 `internal/jsonrpc` 的 `HTTPClient` 访问标准
   JSON-RPC 2.0 over HTTP 节点；本地也可以用 `jsonrpc.NewHandler` 把同一个
   假节点以同样协议架在 localhost 上，用于复现与验证。

纯标准库实现，无第三方依赖。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/types`   | `Block` / `Event` / `ConfirmationRule`（确认规则） |
| `internal/rpc`     | `Client` 接口、错误分类（超时/临时/限流/永久）、有上限的退避重试 |
| `internal/jsonrpc` | HTTP JSON-RPC 2.0 传输：`HTTPClient`（接真实节点）+ `Handler`（把任意 `rpc.Client` 架成节点） |
| `internal/fakenode`| 可编程假节点：装链、脚本化故障、注入延迟、切换分叉、记录调用与并发峰值 |
| `internal/store`   | 内存索引库：顺序写入、幂等去重、最终化标记、回退/重置 |
| `internal/indexer` | 核心编排：增量拉取、确认推进、重组检测与回退、并发上限与顺序写入 |
| `cmd/indexer-demo` | 本地可运行演示：限流重试 + 重复拉取 + 重组三阶段（`-http` 走网络） |

## 快速验证

```bash
go build ./...
go test ./... -race -count=1          # 全量单测（含竞态检测）
go test ./... -count=10               # 重复执行，验证并发路径稳定性
go run ./cmd/indexer-demo             # 进程内假节点，打印每个高度、哈希与判定依据
go run ./cmd/indexer-demo -http       # 同一假节点改走 HTTP JSON-RPC，结果应完全一致
go run ./cmd/indexer-demo -http -confirmations=0  # 确认数 0：立即呈现，重组仍被纠正
```

单测使用 `t.Logf` 打印处理过程，`go test -v` 可看到形如：

```
height=7 hash=0xa833.. decision=index parent=0xc3d0.. events=1
height=7 stored-hash=0x63cb.. node-hash=0xa833.. decision=tail-mismatch reason=same-height-reorg
height=6 hash=0xc3d0.. decision=reorg-probe result=common-ancestor
reorg decision=rollback common-ancestor=6 removed=3
height=7 hash=0xa833.. decision=finalize reason=tip-4 >= confirmations-3
```

## 索引与确认规则

- 每个区块以 `(number, hash, parentHash, events)` 表示；事件唯一键为
  `blockHash#eventIndex`，同一区块重复拉取不会产生重复事件记录。
- **幂等**：同一高度写入哈希完全相同的区块是空操作（标记 `duplicate`，无新记录）；
  同一高度出现不同哈希返回 `store.ErrConflict`。
- **连续写入**：只接受高度恰好为 `head+1` 的区块，跳跃写入返回
  `store.ErrGap` / 索引层的 `indexer.ErrHeightJump`，高度与请求不一致同样拒绝。
- **确认数**：`ConfirmationRule{Confirmations: N}` 规定区块 `h` 仅在
  `tip - h >= N` 时才允许 `finalize`。未确认区块只在内部暂存，
  **不会**出现在 `Store.FinalBlocks()`（对外最终结果）中；后续链增长时按
  高度顺序补做最终化。
- **缺数据**：节点声明 tip 已覆盖某高度但返回 `block not found`，返回
  `indexer.ErrMissingData`；与重组无法区分时先探测共同祖先再下结论。

## 链重组回退策略

每轮运行时，除了向前拉新区块，还会校验未最终化尾部与节点是否一致，因此
三类信号都能识别为重组：

1. **父哈希不连续**：新区块的 `parentHash` 不指向已索引的前一区块；
2. **高度回退**：节点 tip 小于本地 head（旧长分支被整条废弃）；
3. **同高换哈希**：tip 没动，但某个未确认高度的哈希被节点替换。

处理流程：

1. 从信号点向下逐个比较「本地哈希 vs 节点哈希」，找到第一个相同的
   **共同祖先（common ancestor）**；
2. 将共同祖先之后的本地区块及其事件**整段删除**（`RollbackTo`），
   丢弃分支不留任何残留；找不到共同祖先时 `Reset` 后从创世重建；
3. 从共同祖先开始按新链重新索引。

**一致性保证**：每一轮跑完后，本地索引状态必须与节点当前链一致；不一致
就回退到共同祖先按新链重新索引。重组收敛后的索引结果，与「直接拿新链从
创世开始索引」逐区块比较哈希、事件与最终化标记完全一致（见
`TestReorg_RollbackToCommonAncestorAndEquivalence` 与 HTTP 接入下的
`TestHTTP_Reorg_ConvergesToNodeChain`）。重组轮数有上限（默认 16），超过
返回 `indexer.ErrReorgNotConverging`。

**确认数为 0 的语义**：`Confirmations: 0` 表示区块一落库即可对外呈现。
此时没有最终化承诺需要保护，因此每轮运行会校验**全部**已存高度（而非仅
未确认尾部）：哪怕高度没变只是同高换了哈希、哪怕该高度早已作为最终结果
给出去过，后续运行也会发现并按节点当前链纠正（`ForceRollbackTo`），绝不
出现「返回成功但索引与节点链不一致」（见
`TestReorg_ZeroConfirmations_SameHeightSwapCorrected`、
`TestReorg_ZeroConfirmations_DeepSwapBelowPresentedHead`）。

**最终化保护（确认数 > 0）**：回退不得跨越最终化高度。如果重组分叉点比
`确认数 N` 还深（意味着已对外呈现的最终结果要被推翻），返回
`indexer.ErrFinalityReorg` 显式失败，且已最终化数据保持不变——绝不静默改写
（见 `TestReorg_PositiveConfirmations_FinalityProtectionIntact`）。

## 网络接入（HTTP JSON-RPC）

`internal/jsonrpc` 定义了一个最小的 JSON-RPC 2.0 over HTTP 协议：

| 方法 | 参数 | 结果 |
| --- | --- | --- |
| `chain_tip` | `[]` | 链头高度（`0x` 十六进制数量） |
| `chain_getBlockByNumber` | `["0x<高度>"]` | 区块对象（number/hash/parentHash/events） |

- **接真实节点**：`jsonrpc.NewHTTPClient(endpoint)` 返回一个 `rpc.Client`，
  外面再包 `rpc.NewRetryClient` 即可直接交给 indexer，与进程内用法相同。
- **本地假节点**：`jsonrpc.NewHandler(node)` 把任意 `rpc.Client`（如
  `*fakenode.Node`）变成 `http.Handler`，用 `httptest` 或 `http.Server`
  起在 localhost 即可。假节点的脚本化故障（`FailNext`/`FailAlways`）会被
  映射为对应的 JSON-RPC 错误码，因此**错误注入语义跨网络保持不变**。
- **兼容范围**：任何实现了上述两个方法、并以相同错误码约定应答的
  JSON-RPC 2.0 节点都可以接入；请求体上限 1 MiB，仅接受 POST。

## 错误类别与节点侧错误的对应关系

错误在 `internal/rpc/errors.go` 中分为**可区分**的四类；HTTP 传输层的
失败与节点返回的 JSON-RPC 错误码按下表归到同一套类别：

| 类别 | 传输层（HTTP/网络） | JSON-RPC 错误码 | 是否重试 |
| --- | --- | --- | --- |
| `ClassTimeout`     | 请求超时（context 截止、net timeout）、HTTP 408 | `-32004` | 是 |
| `ClassTemporary`   | 连接失败/重置/DNS、HTTP 5xx、响应体损坏 | `-32603`、`-32000` | 是 |
| `ClassRateLimited` | HTTP 429 | `-32005` | 是 |
| `ClassPermanent`   | 其他 HTTP 4xx、结果体格式错误 | `-32700`/`-32600`/`-32601`/`-32602`、`-32001`（`ErrBlockNotFound`） | 否 |

- `RetryClient` 对可重试错误执行**有上限**的重试（`MaxAttempts`，含首次），
  采用指数退避 + 抖动（`BaseBackoff` 倍增、封顶 `MaxBackoff`）。
- 超过上限返回包装了最后一次错误的 `rpc.ErrRetryExhausted`（可用
  `errors.Is` 判断、`errors.As` 取尝试次数与末次错误），**显式失败，
  绝不跳过该高度继续**。
- 永久错误（数据缺失等）立即返回，不浪费重试。
- 同一次运行中多种错误混合出现时各自归类：可重试的消耗重试次数，
  一旦出现永久错误立即终止（见 `TestHTTP_RetryThenSuccess_And_Exhaustion`
  的 mixed-failures 用例）。

## 节点异常与限流（进程内语义）

**并发与写入顺序**：

- 拉取侧用容量为 `Concurrency` 的信号量限流；`fakenode` 记录
  `PeakInFlight`，测试断言并发不超上限且确实并发。
- 一批区块**并发拉取、按高度顺序收集结果、顺序校验并写入**，写入顺序不受
  goroutine 调度影响（`TestWriteOrderIndependentOfScheduling` 以 50 个区块、
  8 并发、注入延迟校验完整父子链）。
- 某拉取失败后，未开始的槽位标记为 `skip` 而不是继续压节点；最终向上返回
  的是导致批处理中止的**真实错误**。

## 假节点可编程能力

```go
node := fakenode.New()
node.SetChain(fakenode.BuildLinear("A", 0, 10)...)   // 装一条线性链
node.SetTip(9)                                        // 单独覆盖 tip
node.FailNext("blk", 2, rpc.ClassRateLimited, 1)      // 下 1 次取 2 号块限流
node.FailAlways("tip", -1, rpc.ClassTimeout)          // tip 永久超时
node.SetDelay(2 * time.Millisecond)                   // 注入延迟（观察并发）
fork := fakenode.BuildFork("A", 6, 6)                 // 从高度 6 分叉
node.ApplyReorg(6, fork...)                           // 节点切换到分叉
node.PeakInFlight() / node.Calls()                    // 并发峰值 / 调用日志
```

`BuildLinear` / `BuildFork` 用 FNV 基于「链身份 + 高度」生成确定性哈希，
分叉块的父哈希自动接到共同祖先上，分叉两侧同高度哈希必然不同。

把假节点架到网络上（与真实节点同一接入路径）：

```go
srv := httptest.NewServer(jsonrpc.NewHandler(node))       // 本地 HTTP JSON-RPC 节点
client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), rpc.DefaultRetryPolicy())
// client 与进程内的 node 行为一致，可直接交给 indexer
```

## 主要错误一览

| 错误 | 触发原因 |
| --- | --- |
| `rpc.ErrRetryExhausted` | 可重试错误超过 `MaxAttempts` |
| `rpc.ErrBlockNotFound`  | 节点在该高度没有区块（永久、不可重试） |
| `indexer.ErrMissingData` | 声明有数据但取不到，且排除了重组 |
| `indexer.ErrHeightJump` | 高度不连续或返回区块号与请求不符 |
| `indexer.ErrParentMismatch` | 父哈希不连续（重组信号，内部自动处理） |
| `indexer.ErrFinalityReorg` | 重组试图回退已最终化区块 |
| `indexer.ErrReorgNotConverging` | 有界重组轮数内未收敛 |
| `store.ErrGap` / `store.ErrConflict` / `store.ErrFinalizedRollback` | 存储层拒绝原因 |
