# chain-reorg-indexer-go

面向区块链节点的本地 JSON-RPC 增量索引组件。用一个**可编程的本地假节点**
（`internal/fakenode`，基于 `httptest` 的 JSON-RPC 2.0 over HTTP）模拟链数据，
让「重复拉取」「链重组（reorg）」「节点超时 / 临时故障 / 限流」三类行为
完全可预期、可复现，无需任何外部服务。

## 包结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | `Block` / `Event` / `ChainHead` 领域类型 |
| `internal/rpc` | JSON-RPC 2.0 HTTP 客户端：错误分类、超时、有限重试 + 指数退避 |
| `internal/fakenode` | 可编程假节点：规范链、重组、故障脚本（超时/临时/限流/缺块/跳高度）、延迟 |
| `internal/store` | 索引存储接口与内存实现：幂等写入、连续校验、回退删除 |
| `internal/indexer` | 核心编排：确认策略推进、去重、跳跃拒绝、共同祖先回退、并发有序写入 |
| `cmd/demo` | 本地端到端演示（初始索引 → 限流 → 重组） |

## 索引与确认规则

- 每个周期先调用 `eth_getChainHead` 读取节点链头，再按高度推进。
- **确认数（Confirmations）**：区块 `h` 仅当
  `h + Confirmations <= tip.Number`（即其后至少埋了 `Confirmations` 个块）
  才会写入「已确认存储」对外呈现。确认数不足的区块只停留在内存未确认窗口，
  绝不会作为最终结果暴露。`Confirmations=0` 表示链头即最终。
- **去重（重复拉取不产生重复记录）**：
  - 已确认存储按 `(height, hash)` 幂等：完全相同的数据重复写入是 no-op；
  - 未确认窗口带缓存，同一未确认区块命中缓存不会重复发 RPC；
  - 每轮都会用节点当前规范哈希校验缓存，命中旧分叉立即作废重拉。
- **高度跳跃 / 数据缺失必须被明确拒绝**，并给出可区分原因：
  - 节点在该高度返回空（JSON-RPC `block not found`）→ `ErrHeightGap`；
  - 节点返回的区块号与请求高度不一致（跳高度/编号错乱）→ `ErrHeightGap`；
  - 区块负载为空 → `ErrHeightGap`；
  - 父哈希接不上前一块 → `ErrParentHashMismatch`（走重组路径，而非静默跳过）。
- **并发上限与写入顺序**：`MaxConcurrency` 限制同时在途的区块拉取请求数，
  但校验/入窗口严格按高度升序进行（producer/collector 模型），
  worker 调度顺序不影响最终写入顺序。

## 链重组（reorg）回退策略

检测到两种分叉信号：

1. **父哈希不连续**：新区块的 `parentHash` ≠ 前一块哈希；
2. **同高度链头哈希变化**：链头高度未增加但链头哈希变了（等长分叉），
   无法靠延伸时的父子链接发现，因此对「已确认链头哈希 vs 节点链头哈希」做显式比对。

处理流程：

1. 从分叉锚点向下逐高度探测，比较「本地已确认哈希」与「节点当前规范哈希」，
   第一个相同的高度即**共同祖先**（探测日志：`ANCESTOR-PROBE ... match=true`）。
2. `RollbackTo(共同祖先)` 删除该高度之上的**全部区块与事件行**，
   同时丢弃未确认窗口中属于旧分叉的缓存；
   保证不残留任何被丢弃分支的数据（日志：`ROLLBACK to=...`）。
3. 沿新链重新抓取并按确认规则写入。
4. **深度限制**：向上回溯超过 `MaxReorgDepth` 个已确认块仍未找到共同祖先，
   显式返回 `ErrReorgTooDeep`，绝不静默截断历史。
5. 重组只发生在未确认窗口时，处理对外透明，已确认数据不变。

不变量（由测试断言保证）：**重组后的最终状态 == 直接在新链上从零索引的结果**，
区块数、事件数、每个高度的哈希与事件完全一致。

## 错误分类与重试

JSON-RPC 客户端把故障归类为可 `errors.Is` 区分的哨兵错误：

| 哨兵 | 触发 | 是否重试 |
| --- | --- | --- |
| `rpc.ErrTimeout` | 客户端超时 / `net.Error.Timeout()` | 是 |
| `rpc.ErrTemporary` | 连接重置、拒绝连接、JSON-RPC `-32603` 等临时故障 | 是 |
| `rpc.ErrRateLimited` | HTTP 429 或 JSON-RPC `-32005`（限流） | 是 |
| `rpc.ErrBlockMissing` | JSON-RPC `-32001`（该高度无块） | 否（立即失败） |
| `rpc.ErrBadResponse` | 非法 JSON / 不可解析的响应 | 否 |

- 重试只针对可重试错误，上限为 `Config.MaxRetries`（总尝试次数 = `MaxRetries+1`），
  退避为指数退避：第 `i` 次重试等待 `BackoffMillis * 2^(i-1)`。
- **超过重试上限显式失败**：错误形如
  `indexer: retry budget exhausted: rpc: eth_getBlockByNumber failed after N attempt(s): rpc: ...`，
  并且同时可用 `errors.Is(err, indexer.ErrRetryExhausted)` 与
  `errors.Is(err, rpc.ErrTimeout / ErrTemporary / ErrRateLimited)` 判定，
  原因可区分、绝不静默跳过。
- indexer 层另区分：`ErrHeightGap`（跳跃/缺块）、`ErrParentHashMismatch`
  （父哈希断裂）、`ErrReorgTooDeep`（重组超深）、`ErrChainBehind`
  （节点链头高度回退到本地已确认高度之下）。

## 假节点脚本能力

```go
node := fakenode.New(fakenode.BuildChain(0, 8, "", "a")) // 初始规范链
node.SetReorg(5, forkBlocks)        // 用 forkBlocks 替换 5 之上的规范链尾
node.Extend(moreBlocks)             // 在链尾顺延
node.SetFailure("eth_getBlockByNumber", fakenode.FailRateLimit, 2) // 下 2 次调用限流
node.SetFailure("eth_getChainHead",  fakenode.FailTimeout,   1)    // 下 1 次超时
node.SetFailure(method, fakenode.FailTemporary, n)                  // 临时故障
node.SetFailure(method, fakenode.FailNotFound,  n)                  // 缺块
node.SetFailure(method, fakenode.FailureMode(fakenode.FailureSkipHeight), n) // 返回下一号块
node.SetLatencyMillis(50)           // 统一注入延迟
node.Calls()                        // 读取 (method, height) 调用序列
```

## 本地验证

```bash
# 全量单元测试（假节点覆盖重复拉取 / 重组 / 限流重试 / 超时 / 缺块 / 跳高度）
go test ./...

# 竞态检测 + 多次重复（并发有序写入场景）
go test -race -count=2 ./...

# 查看每个高度、区块哈希与判定依据（FETCH / CACHED / CONFIRM /
# PENDING-INVALIDATED / MISMATCH / ANCESTOR-PROBE / ROLLBACK / REJECT）
go test -v ./internal/indexer/

# 端到端可复现演示：初始索引 -> 两次限流 -> 重组 -> 再索引
go run ./cmd/demo
```

## 主要测试场景

- `internal/store`：幂等重放、重复哈希拒绝、高度跳跃拒绝、父哈希拒绝、回退清除与重建。
- `internal/rpc`：临时故障/限流后自动恢复、超限显式失败、超时与临时故障可区分、
  缺块不重试、三类原因互斥可区分。
- `internal/indexer`：
  - 确认策略：未确认区块/事件不对外；链增长后按确认数推进；
  - 重复拉取：多轮空闲周期后区块/事件数不变；
  - 缺块 / 跳高度：分别以 `ErrHeightGap` 明确拒绝，且与重组原因区分；
  - 未确认窗口内重组、已确认区间重组、等长分叉替换链头；
  - 重组后状态与「新链从零索引」逐项一致，无旧分支残留；
  - 重组超过 `MaxReorgDepth` 显式失败；节点链头高度回退显式失败；
  - 限流重试成功、持久限流/临时故障耗尽预算显式失败且保留底层原因；
  - 高并发（8 worker）下写入仍为无缺口、父子相连的有序链（`-race` 验证）。
