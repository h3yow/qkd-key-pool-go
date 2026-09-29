# qkd-key-pool-go（本地可复现 QKD 协商与密钥池模拟）

一个**纯本地、确定性可复现**的量子密钥协商（QKD）会话与密钥池服务模拟，用于：

- 模拟两个 QKD 节点建会话，经历 Hello → Sift（筛基）→ 参数估计（QBER 门限）→ 确认 → 建链；
- 在**可配置的链路扰动、丢包、探测噪声**下验证异常链路：误码率过高、确认失败、对端中止、超时、丢帧；
- 提供密钥**租用 / 确认 / 释放 / 超时回收**，保证并发互斥、消费即失效、过期不重发；
- 处理**协议版本与异常消息**：重复、乱序、过期（stale）、版本不兼容都给出**可区分**的拒绝，且**不把底层内部错误原样外泄**。

> 不依赖任何真实量子设备、网络服务或真实凭据。链路是进程内两个端点之间传递的帧，
> 随机性来自可播种的 PRNG（`math/rand/v2` PCG），密钥材料是本地生成的随机字节。

## 包结构

| 包 | 职责 |
|---|---|
| `qkderr` | 稳定、可区分的错误分类（`Kind`）；`Internal/Wrap` 隐藏底层原因，仅经 `Unwrap` 供日志使用 |
| `proto` | 协商消息、协议版本、步骤序号（Seq）、结构/版本校验 |
| `link` | 本地量子/经典链路模拟：丢包重试、由扰动+噪声推导 QBER、端点收件箱 |
| `session` | 协商状态机：发起方/响应方两角色，明确的终态与可区分失败原因 |
| `pool` | 并发安全密钥池：`Lease / Confirm / Release / ReapOnce / RunReaper` |
| `cmd/qkd-demo` | 命令行端到端演示，参数可配、种子可复现 |

## 快速开始

```bash
go build ./...
go vet ./...
go test ./... -race -v     # 详细日志：关键输入、状态变化、判定依据

# 正常协商 + 租用/确认
go run ./cmd/qkd-demo -seed 1

# 异常链路（每种都停在不同终态、给出不同 reason）
go run ./cmd/qkd-demo -disturb 0.3       # 误码率过高 -> aborted-qber
go run ./cmd/qkd-demo -peer-abort        # 对端中止     -> aborted-peer
go run ./cmd/qkd-demo -peer-noconfirm    # 确认失败     -> aborted-confirm
go run ./cmd/qkd-demo -peer-version qkd-sim/0.9  # 版本不兼容 -> aborted-protocol
go run ./cmd/qkd-demo -loss 1            # 严重丢包     -> aborted-confirm(dropped)
```

## 会话状态流转

```
 init
  │ hello
  ▼
 hello-sent ──(收到 abort, 版本原因)──► aborted-protocol   [unsupported-version]
  │ sift
  ▼
 sifting ───(对端 abort)────────────► aborted-peer        [peer-aborted]
  │
  ▼
 estimating ─(QBER > 阈值)──────────► aborted-qber        [qber-too-high]
  │        └(对端 QBER > 阈值)──────► aborted-qber
  ▼
 confirming ─(未收到 confirm/超时)──► aborted-confirm     [confirm-failed]
           ─(对端 abort 确认失败)──► aborted-confirm
  │ confirm
  ▼
 established
 (任一阶段) 上下文/步骤超时 ─────────► aborted-timeout     [timeout]
 (任一阶段) 丢帧超过最大重试 ───────► aborted-confirm     [dropped]
 (任一阶段) 未归类内部故障 ─────────► failed / aborted-*   [internal]（原因不外泄）
```

每个停止点都是**终态**（`State.Terminal()==true`），调用方用
`n.State()` 看卡在哪一步、`qkderr.KindOf(n.Err())` 拿机器可判定的失败类别，二者可区分。

### 异常消息不静默忽略

`Negotiation.HandleMessage` 是唯一入口，按顺序判定：

1. 结构/版本校验 → `malformed` / `unsupported-version`；
2. 会话不匹配或会话已终止后才到 → `stale-message`；
3. 步骤序号小于已接收序号 → `duplicate-message`；
4. 步骤序号大于期望序号 → `out-of-order-message`。

## 密钥池安全边界

- **互斥租用**：选 key 与翻转为 `leased` 在同一把锁内原子完成，N 个并发请求抢同一 key，**恰好一个成功**，其余拿到 `key-unavailable`。
- **消费不可重发**：`Confirm` 后 key 进入 `consumed`，永久不再被 `Lease` 选中；对同一租约重复确认返回 `lease-not-held`。
- **过期不可发放**：材料到期（`expiry`）的 key 即使处于 available 也会懒/显式转为 `expired`，永不发放；对过期 key 确认返回 `key-expired`。
- **租约 TTL 回收**：租约有独立 TTL。`ReapOnce`/后台 `RunReaper` 到期回收——材料仍有效则退回 `available` 供他人租用，材料到期则转 `expired`；被回收的租约再确认/释放返回 `lease-not-held`，TTL 内迟到的确认返回 `timeout`。
- **释放语义**：仅 `leased` 可 `Release` 退回；`consumed/expired` 不可释放。
- **异常可解释**：所有拒绝都带 `qkderr.Kind`，状态可由 `KeyStateOf/Len/ActiveLeases` 复核。
- **边界声明**：这是模拟。密钥材料是本地随机字节，不提供量子级安全保证、不做真实认证/加密信道；不要用于生产或承载真实秘密。内部错误（如随机源/存储错误）经 `qkderr.Internal/Wrap` 归类，`Error()` 不渲染底层细节。

### 密钥生命周期

```
 add ─► available ──Lease──► leased ──Confirm──► consumed   (终态)
            ▲                  │
            │                  ├─Release────────► available
            │                  └─TTL 回收(材料有效)► available
            └─(材料到期 / TTL 回收时材料也到期)──► expired (终态)
```

## 关键配置项

### 链路 `link.Config`
| 字段 | 含义 |
|---|---|
| `LossRate` | 经典帧每次投递的丢失概率（0..1），失败按 `MaxAttempts` 重试，耗尽 → `dropped` |
| `Disturbance` | 信道扰动/窃听率，主导 QBER：调高即可稳定触发误码中止 |
| `DetectorNoise` | 探测噪声率，以 0.3 权重计入 QBER |
| `BaseQBER` | 内在误码底噪 |
| `Seed` | PRNG 种子，保证同种子可复现 |
| `MaxAttempts` | 丢帧最大重试次数 |

QBER 近似：`base + disturbance + 0.3*detectorNoise + 抖动(±0.2%)`，截断到 [0,1]。

### 会话 `session.Config`
`QBERThreshold`（超过即 abort）、`StepTimeout`（单步超时）、`SessionID`，
以及对端故障钩子 `PeerForceAbort / PeerFailConfirm / PeerVersion`（用于复现各类失败）。

### 密钥池 `pool.Config`
`LeaseTTL`（租约时长）、`ReapInterval`（后台回收周期）、`Now`（可注入时钟，便于确定性测试）。

## 本地验证方法

```bash
# 全量单测（含并发竞态检测），-v 可见关键输入与状态变化日志
go test ./... -race -v

# 单独跑某场景
go test ./session/ -run HighQBER -v
go test ./pool/   -run ConcurrentLease -race -v
go test ./pool/   -run LeaseTTL -v

# 可复现的端到端 CLI（改 -seed 复现同一随机序列）
go run ./cmd/qkd-demo -disturb 0.3 -seed 7
```

测试覆盖：高误码率中止、对端中止、确认失败、版本不兼容、丢包、
重复/乱序/过期/版本消息分类、并发抢用唯一赢家、租约超时回收、过期不发放、释放复用、后台回收，
以及“内部错误文本不外泄”。失败断言统一用 `qkderr.KindOf(err)` 区分原因。
