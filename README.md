# qkd-key-pool-go

纯本地、可复现的**量子密钥协商（QKD）会话**与**密钥池**模拟器。用于建模 QKD 节点之间建会话、
分发/消费密钥以及处理异常链路：不连接真实量子设备、不访问外部服务、不需要任何真实凭据。

- 协商：BB84 风格的制备—传输—测量—筛选—误码估计—确认流程；链路丢包率、探测噪声、协议版本与
  对端行为均可配置；每一步都有明确状态与可区分的失败码。
- 密钥池：密钥的租用 / 取材料 / 确认消费 / 释放 / 超时回收；并发抢同一把钥匙只有一个赢家；
  已消费、已释放、已过期的密钥永不再发放。
- 消息纪律：重复、乱序、过期、版本不兼容的协商消息一律给出**可区分的拒收结果**，绝不静默忽略；
  内部错误不跨边界泄露。

## 目录结构

```
internal/qkd/
  errors.go    协商失败码 FailureCode 与可脱敏的 *Failure
  protocol.go  协议版本、协商消息、TTL 与严格序号守卫 OrderGuard
  channel.go   可复现的模拟量子链路（丢包 + 噪声翻转，种子驱动）
  session.go   会话状态机（hello→parameters→transmission→sifting→estimating→confirming→established）
internal/pool/
  key.go       密钥实体与生命周期状态
  pool.go      互斥的密钥/租约管理与后台回收器
cmd/qkddemo/   本地演示：正常/高误码/对端中止/版本不兼容协商 + 并发抢用密钥池
```

## 协商会话状态流转

```
idle
  └─▶ hello ───────────────▶ parameters ──▶ quantum_transmission ──▶ sifting
        │ 版本不兼容中止           │ 对端中止       │ 对端中止            │ 对端中止 / 全丢包无剩余比特
        └──────────────┬──────────┴────────────────┴──────────────────────┘
                       ▼
                    aborted  ◀── estimating（QBER 超阈值中止）
                       ▲        confirming（对端不确认 / 对端中止）
                       │
sifting ─▶ estimating_qber ─▶ confirming ─▶ established（拿到共享密钥）
```

每个状态推进都产生一条 `StepEvent`（时间戳 + 状态 + 判定依据，例如
`qber=120/773=0.1552 threshold=0.1500`），失败时停在 `aborted`，并在 `Failure.Step`
中记录**卡在哪一步**，`Failure.Code` 记录**为什么**：

| FailureCode            | 含义                         | 典型卡点 Step      |
|------------------------|------------------------------|--------------------|
| `qber_too_high`        | 误码率超过 `QBERThreshold`   | `estimating_qber`  |
| `confirmation_failed`  | 确认阶段对端未回 ack         | `confirming`       |
| `peer_aborted`         | 对端主动中止                 | 中止发生时的状态   |
| `version_incompatible` | 对端协议版本不受支持         | `hello`            |
| `no_key_agreed`        | 筛选后没有存活比特（如全丢） | `sifting`          |
| `internal`             | 内部不变量破坏/取消          | 发生时的状态       |

> **脱敏**：`Failure.Error()` 面向本地日志（含内部 cause）；跨信任边界请调用 `Failure.Public()`，
> 它会剥离内部 cause 并把 `internal` 细节泛化，调用方仍可依据 `Code`/`Step` 分支处理。

### 模拟链路与误码

`LinkConfig{LossRate, NoiseRate, Seed}`：

- `LossRate`：每个光子被丢弃的概率（模拟链路扰动/丢包）；
- `NoiseRate`：存活光子被翻转的概率（模拟探测噪声，直接贡献 QBER）；
- `Seed`：随机种子。同种子 + 同输入得到逐光子一致的结果，测试可复现。

QBER 定义为“基一致的筛选比特中，Alice/Bob 比特不一致”的比例，严格大于
`QBERThreshold` 即中止（缺省 0.15）。

## 协商消息纪律

入站消息经 `OrderGuard.Check`，按顺序做四类检查并返回不同的 `RejectCode`：

| RejectCode            | 触发条件                                   |
|-----------------------|--------------------------------------------|
| `version_incompatible`| `Version` 不在 `SupportedVersions()` 中    |
| `message_expired`     | 当前时间晚于 `IssuedAt + TTL`              |
| `duplicate_message`   | 序号已应用或落后于窗口                     |
| `out_of_order`        | 序号跳过期望值（严格递增，不允许跳号）     |

拒收不推进守卫状态（版本/过期消息之后的修正消息仍可被接受），且不会被静默吞掉。

## 密钥池：生命周期与安全边界

```
AddKey ─▶ available ──Lease──▶ leased ──Confirm──▶ confirmed（消费，永不复用）
              │                   │
              │ 到期              ├──Release──▶ released（作废，不回收复用）
              ▼                   └──到期/回收─▶ expired
          expired
```

安全边界（这是模拟器，不是生产密钥管理系统）：

- **单一互斥点**：所有状态变更都在池内一把 `sync.Mutex` 下完成，因此“N 个会话并发抢同一把
  钥匙，恰好 1 个成功，其余得到 `empty_pool`”是结构性保证（`-race` 覆盖）。
- **材料最小暴露**：`Key` 字段包内私有；`Material()` 仅在**租约有效期内**返回材料副本；
  入池与读出都做拷贝，调用方无法共享/篡改池内底层数组；错误信息只含 id，不含材料。
- **终态不可逆**：`confirmed` / `released` / `expired` 的密钥永远不会再次发放；
  租约一旦注销（确认、释放、超时），无法再用旧租约取材料或重复确认
  （分别得到 `lease_not_found` / `lease_expired` 等可区分错误）。
- **租约不超过密钥寿命**：`Lease.ExpiresAt = Key.expiresAt`。
- **超时可解释**：后台回收器（`RunReaper`）与懒回收都会把过期对象落为 `expired`；
  异常退出（拿到租约后既不确认也不释放）的最终状态同样是 `expired`，可从
  `KeyStatus` / `LeaseInfo` 解释。

| ErrorCode          | 含义                                       |
|--------------------|--------------------------------------------|
| `empty_pool`       | 没有可租用（available 且未过期）的密钥     |
| `lease_not_found`  | 租约不存在或已注销                         |
| `lease_expired`    | 租约/密钥已过期                            |
| `key_consumed`     | 密钥已确认消费                             |
| `already_released` | 密钥已释放                                 |
| `key_expired`      | 密钥已过期                                 |
| `key_not_leased`   | 目标密钥不在 leased 状态                   |

## 关键配置项

| 位置                  | 配置                  | 说明                                   | 缺省        |
|-----------------------|-----------------------|----------------------------------------|-------------|
| `qkd.SessionConfig`   | `PhotonCount`         | 制备光子数                             | 1024        |
|                       | `QBERThreshold`       | 误码率上限，超过即中止                 | 0.15        |
|                       | `Link.LossRate`       | 链路丢包率 [0,1]                       | 0           |
|                       | `Link.NoiseRate`      | 探测噪声/翻转率 [0,1]                  | 0           |
|                       | `Link.Seed` / `Seed`  | 随机种子（链路与基选择），保证可复现   | 0           |
|                       | `MessageTTL`          | 协商消息有效期                         | 5s          |
|                       | `ForceConfirmFailure` | 注入：确认阶段对端不确认               | false       |
|                       | `PeerAbortAfter`      | 注入：对端进入某状态后中止             | 不中止      |
|                       | `PeerVersion`         | 对端通告版本（用于版本不兼容场景）     | 与本端相同  |
| `pool.Config`         | `DefaultTTL`          | 新密钥寿命，也是租约上限               | 30s         |
|                       | `ReapInterval`        | 后台回收间隔                           | 100ms       |

时间来源：会话通过 `SessionConfig.Now` 注入，密钥池所有方法都接收 `now time.Time`
参数，便于在测试里用确定的假时钟验证过期与回收。

## 本地验证

需要 Go 1.25+（模块声明 `go 1.25.0`）。

```bash
# 构建
go build ./...

# 全量测试（带竞态检测）
go test -race ./...

# 查看协商/池测试中的关键输入、状态变化与判定依据
go test -v ./internal/qkd/
go test -v ./internal/pool/

# 运行演示：正常、高误码中止、对端中止、版本不兼容 + 并发抢用
go run ./cmd/qkddemo
```

测试覆盖：

- 高误码率超过阈值 → `qber_too_high` 停在 `estimating_qber`；
- 确认失败 / 对端中止 / 版本不兼容 / 全丢包无密钥 → 各自失败码与卡点状态；
- 16 个 goroutine 并发抢同一把钥匙 → 恰好 1 个赢家，其余 `empty_pool`；
- 租约 TTL 到期：主动操作报 `lease_expired`、后台回收器注销租约并把密钥置 `expired`；
- 消息重复 / 乱序 / 过期 / 版本不兼容 → 四种可区分拒收，且拒收不推进序号窗口；
- 材料隔离：调用方篡改取到的副本不影响池内材料。
