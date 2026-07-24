# PrizeForge 工程问题与解决记录

> 更新时间：2026-07-24

本文持续记录项目开发和压测过程中实际遇到的问题。每项只保留现象、排查、原因、解决方案和验证结果，作为后续复盘与面试说明的依据。

## 1. 策略奖品库存热点行锁导致慢 SQL

### 现象

在 `2C4G` 单机环境中，并发从 25 提高到 40 后，QPS 只从 `81.04` 增长到 `82.51`，平均延迟却从 `308ms` 增长到 `484ms`。MySQL CPU 一度达到 `93%`，并反复出现以下慢 SQL：

```sql
UPDATE strategy_award
SET award_count_surplus = award_count_surplus - 1
WHERE strategy_id = ?
  AND award_id = ?
  AND award_count_surplus > 0;
```

### 排查

结合 `docker stats`、`vmstat`、GORM 慢 SQL 和 Outbox 状态统计，确认 RabbitMQ 没有积压，主要压力集中在 MySQL。进一步检查代码发现，每条 `strategy_award_stock_sync` 任务都会启动一次事务，并更新相同的策略奖品库存行。

### 原因

同一个热门奖品对应同一条 `strategy_award` 记录。大量任务并发执行单行 `UPDATE` 时，只能串行获得该行的排他锁，形成热点行锁竞争。

### 解决方案

扫描 Outbox 后按照 `(strategy_id, award_id)` 分组，在一个事务内写入订单维度的幂等凭证，只统计本批首次出现的订单数量 `N`，最后执行一次：

```sql
award_count_surplus = award_count_surplus - N
```

同时使用有界工作池，避免为每条任务创建无上限 goroutine。

### 验证

相同测试环境下，并发 25 的 QPS 从 `81.04` 提升到 `112.37`，平均延迟从 `308ms` 降至 `222ms`；各档位 QPS 提升约 `35%～39%`，平均延迟下降约 `26%～28%`。优化后的慢 SQL 中不再出现原来的单条库存扣减热点。

## 2. Outbox 状态逐条更新造成数据库写放大

### 现象

Outbox 扫描器处理一批任务后，会为每个任务分别执行一次 `completed` 或 `fail` 状态更新。压测期间任务数量快速增长，数据库需要执行大量小事务和重复索引查找。

### 排查

库存批量同步完成后，慢 SQL 的主要来源从 `strategy_award` 热点库存行转移到 `task` 表状态更新。代码检查确认，任务分发虽然可以并发执行，但结果仍按 `user_id + message_id` 逐条回写。

### 原因

任务处理结果没有按分库汇总。一次扫描 500 条任务，最坏情况下需要额外执行约 500 次状态更新，放大了数据库交互次数和事务开销。

### 解决方案

查询任务时带出主键 `id`，任务执行完成后按照分库分别收集 `completedIDs` 和 `failedIDs`，每 500 条执行一次：

```sql
UPDATE task
SET state = ?
WHERE id IN (...)
  AND state IN ('create', 'fail');
```

状态条件用于避免并发补偿把已经完成的任务重新降级为失败。

### 验证

单元测试和真实 MySQL 集成测试覆盖了分库批量更新、部分成功、部分失败和状态保护。压测中曾观察到 500 条批量更新耗时 `255～355ms`，说明单次大批量更新仍有优化空间，但状态回写 SQL 的数量已经从“每任务一次”降为“每分库每批一次”。

## 3. Redis-first 抽奖结果的可靠落库

### 现象

同步 MySQL 版本可以保证请求返回前完成落库，但完整链路在 `2C4G` 环境中的性能拐点约为 `112 QPS`。如果只把额度扣减改为 Redis Lua 并立即返回，又会出现“Redis 已扣额度，但服务在 MySQL 落库前宕机”的结果丢失窗口。

### 排查

对请求链路逐段分析后，将问题拆成两个目标：

1. 热路径不再同步执行多次 MySQL 写入；
2. 已完成的抽奖结果必须拥有可恢复、可重复投递的持久化依据。

单纯使用 RabbitMQ 仍存在发布前进程崩溃的窗口，因此不能只依赖一次消息发送。

### 原因

性能和可靠性边界没有分开：同步版本把所有数据库工作放在请求内；简单异步版本又缺少 Broker 发布失败或进程宕机后的恢复来源。

### 解决方案

当前 Redis-first 链路采用：

1. Redis Lua 原子校验并扣减总、月、日额度，同时写入 pending 订单；
2. 抽奖完成后，用 Lua 原子保存标准结果并写入 Redis Stream；
3. 发布 RabbitMQ 消息并等待 Publisher Confirm；
4. 消费者在一个 MySQL 事务内保存订单、额度、中奖记录和后续 Outbox；
5. 定时恢复任务扫描 Stream，补发尚未确认的结果；
6. MySQL 依靠 `request_id`、`order_id` 和 `message_id` 唯一约束保证重复消费幂等。

### 验证

单元测试和集成测试覆盖了额度原子预占、结果 Stream、Publisher Confirm、重复结果落库和事务回滚。该架构已随 `v1.1.0` 部署；完整性能结论需要在当前消息重投问题随 `v1.1.1` 修复部署后重新压测。

## 4. RabbitMQ 可靠发奖与状态语义不完整

### 现象

早期 Outbox 任务被更新为 `completed` 后，只能说明代码调用过 RabbitMQ 发布接口，无法证明消息已经被 Broker 接收并路由，也无法证明发奖消费者已经完成中奖记录更新。

### 排查

沿着 `task → RabbitMQ → send_award consumer → user_award_record` 检查状态变化，发现“消息发布成功”和“奖品处理完成”被混在同一个完成概念中，且缺少 `send_award` 的完整消费确认链路。

### 原因

发布端没有完整使用持久化消息、mandatory 路由检查和 Publisher Confirm；消费端也缺少以中奖记录为幂等边界的最终状态确认。

### 解决方案

- 发布消息使用持久化投递；
- 使用 mandatory 检查消息是否真正路由到队列；
- 等待 RabbitMQ Publisher Confirm 后，才将 Outbox 任务标记为已发布；
- `send_award` 消费者处理成功后手动 ACK；
- 消费者以 `user_id + order_id` 幂等更新中奖记录为 `complete`；
- 临时错误 Nack 重试，非法消息拒绝，不把“发布完成”误写成“用户已收到奖品”。

个人项目不接入真实奖品供应商，因此 `award_state=complete` 的边界定义为：系统已经完成内部奖品 ID 的发放确认。

### 验证

RabbitMQ 发布、路由和 Consumer 集成测试均使用真实 RabbitMQ；测试覆盖 Publisher Confirm、不可路由消息、重复消费和最终中奖状态更新。

## 5. 旧抽奖结果重投导致消息无限重试

### 现象

Redis-first 压测后：

```text
draw_result_queue:
messages_ready = 11205
messages_unacknowledged = 1
consumers = 1
```

队列长时间没有变化，同一个订单持续输出：

```text
clear persisted pending draw failed: status=-1
```

### 排查

RabbitMQ 消费者处于 active，MySQL `PROCESSLIST` 没有阻塞 SQL。通过向 Go 进程发送 `SIGQUIT` 获取 goroutine 堆栈，再结合日志确认 `SaveDrawResult` 已经完成 MySQL 幂等落库，失败发生在提交后的 Redis pending 清理阶段。

### 原因

旧结果落库后，用户已经创建了更新的 pending 订单。Lua 脚本发现当前 pending 的 `order_id` 与旧结果不一致，因此返回 `-1` 并正确保留新订单；Go 代码却把这个状态误判为临时失败，导致旧消息不断 Nack、重新入队。

### 解决方案

将 pending 清理结果重新定义为：

- `1`：当前 pending 属于旧结果，已经删除；
- `0`：pending 已不存在，幂等成功；
- `-1`：pending 已属于更新订单，必须保留，并将旧消息视为幂等成功。

同时保留 Lua 的订单号条件判断，确保旧消息永远不能删除新 pending。

### 验证

新增集成测试模拟“旧结果已落库 → 创建新 pending → 重放旧结果”，验证旧结果返回成功、新 pending 保持不变、MySQL 不重复扣额度。目前代码测试已通过，待 `v1.1.1` 部署后观察历史队列自动 ACK 并归零。

## 6. 无限重试引发日志洪水和消费者阻塞

### 现象

同一次压测产生 `130173` 个 `409 DRAW_IN_PROGRESS`，API 日志目录达到约 `33MB`，并持续生成压缩日志。goroutine 堆栈显示消费者阻塞在：

```text
DrawResultListener.Handle
→ logger.Error
→ zap
→ lumberjack.Write
```

### 排查

磁盘只使用约 `20%`，inode 只使用约 `4%`，排除了磁盘空间不足。结合日志内容发现，同一条 `status=-1` 消息在毫秒级循环重试，而且 Listener 和 Consumer 会重复记录错误；HTTP Handler 还把每个预期 409 记录为 ERROR。

### 原因

业务幂等错误触发无限 Nack 后，又被同步文件日志进一步放大。大量 goroutine 争用 lumberjack 的日志写锁，最终让唯一消费者无法及时完成 Ack/Nack。

### 解决方案

- 修复 `status=-1` 的消息幂等语义，消除无限重试源头；
- 去掉 DrawResultListener 和 Consumer 的重复错误日志；
- 将正常抽奖成功和预期 409 日志降为 Debug；
- 为单条 RabbitMQ 消息增加 30 秒处理超时；
- Redis pending 清理沿用消费上下文，避免 `context.WithoutCancel` 绕过超时；
- 保留日志切割，但不再在压测热路径逐请求写入 INFO/ERROR。

### 验证

单元测试验证超时消息会被取消并重新入队，完整集成测试验证消息重投不会再进入错误循环。代码测试已经通过，待 `v1.1.1` 部署后继续验证日志增长速度、队列消化速度和消费者稳定性。

## 7. RabbitMQ 单消费者限制抽奖结果异步落库

### 现象

Redis-first 主链路在并发 25、持续 2 分钟的压测中共处理 `88001` 个请求，但只有 `11776` 个业务成功，另外 `76225` 个请求返回 `409 DRAW_IN_PROGRESS`。同时 `draw_result_queue` 持续积压，且长时间只有一个未确认消息：

```text
draw_result_queue:
messages_ready = 3810
messages_unacknowledged = 1
consumers = 1
```

### 排查

`rabbitmqctl list_consumers` 显示消费者处于 active，但 `prefetch_count=1` 且队列只有一个消费者；MySQL `PROCESSLIST` 没有长时间阻塞 SQL，队列仍只能缓慢下降。检查消费端代码发现，每个 Topic 固定只创建一个 AMQP Channel 和一个消费 goroutine，并硬编码 `prefetch=1`。

压测程序会循环使用 10000 个用户。抽奖结果在异步落库并清除 Redis pending 之前，相同用户再次请求会正确返回 `DRAW_IN_PROGRESS`，因此消费者吞吐不足最终表现为大量 409。

### 原因

Redis-first 将抽奖结果持久化移出请求热路径后，生产速度明显高于单消费者的 MySQL 事务落库速度。单 Channel、单 goroutine、单未确认消息形成消费端串行瓶颈，也让后续 `send_award_queue` 缺少独立的扩容能力。

### 解决方案

- 每个并行消费者使用独立 AMQP Channel 和消费 goroutine，避免共享 Channel 的 ACK 状态；
- 保留手动 ACK/Nack，并让 `prefetch` 对每个 Channel 独立生效；
- 使用统一的队列并发映射，未单独配置的队列使用默认并发数：

```yaml
rabbitmq:
  listener:
    simple:
      prefetch: 1
      default_concurrency: 1
      concurrency:
        draw_result_queue: 8
        send_award_queue: 4
```

- 增加单元测试验证默认值、非法配置回退和队列独立并发；
- 增加真实 RabbitMQ 集成测试，验证同一队列可以由三个独立 Channel 同时处理阻塞消息。

### 验证

`v1.1.3` 部署后，RabbitMQ 显示 `draw_result_queue` 有 8 个消费者、`send_award_queue` 有 4 个消费者。相同 2C4G 环境下：

| 并发 | 时长 | QPS | 业务成功率 | 平均延迟 | P95 |
|---:|---:|---:|---:|---:|---:|
| 10 | 1 分钟 | 101.18 | 100% | 98.653ms | 186.689ms |
| 25 | 2 分钟 | 115.90 | 100% | 215.517ms | 345.730ms |

并发 25 的成功吞吐从约 `98.1 QPS` 提升到 `115.9 QPS`，提升约 `18%`，并消除了该轮压测中的 409。不过压测结束时 `draw_result_queue` 仍积压 `5337` 条消息，说明增加消费者解决了单消费者限制，但异步落库能力仍低于入口流量。

进一步排查发现，所有 RabbitMQ 发布共用一个 Publisher Channel，并在全局互斥锁内等待 Broker Confirm，导致接口并发增加时 QPS 接近不变、延迟持续增长。

## 8. Publisher 全局锁导致 RabbitMQ 发布串行化

### 现象

增加抽奖结果消费者后，接口成功率恢复到 100%，但吞吐没有随并发数明显增长：

| 并发 | QPS | 平均延迟 | P95 |
|---:|---:|---:|---:|
| 1 | 91.17 | 10.933ms | 25.940ms |
| 10 | 101.18 | 98.653ms | 186.689ms |
| 25 | 115.90 | 215.517ms | 345.730ms |

并发从 1 增加到 25 后，QPS 只提高约 27%，平均延迟却增加近 20 倍，表现为明显的串行排队。

### 排查

压测后的 CPU 快照并未持续满载，主要慢 SQL 只是 Outbox 对 `task` 表的批量状态更新，无法解释接口吞吐稳定在约 100 QPS。检查 RabbitMQ Publisher 后发现，所有 Topic 共用一个 AMQP Channel 和一把全局互斥锁，而且锁覆盖了完整的网络等待过程：

```text
加锁
→ 发布持久化消息
→ 等待 Broker Confirm
→ 检查 mandatory return
→ 解锁
```

单次发布确认约需 10ms，因此所有并发请求以及后台 Outbox 发布都在同一把锁后排队，吞吐上限自然接近每秒 100 次。

### 原因

原实现为了防止同一 Channel 上的 Confirm 和 mandatory return 相互串扰，选择将发布过程串行化。可靠性是正确的，但锁的粒度过大：等待 RabbitMQ 网络确认时仍持有全局锁，导致整个进程同一时间只能发布一条消息。

### 解决方案

- 将单 Channel 和全局锁替换为有界 Publisher Channel 池；
- 生产环境配置 `pool_size: 8`，最多允许 8 条消息同时等待 Broker Confirm；
- 每次发布独占一个 Channel slot，同一 Channel 内仍按顺序关联 mandatory return 和 Confirm；
- 发布完成后归还 slot，池满时遵守请求 Context 超时，不无限等待；
- 每个 slot 独立维护已声明 Exchange 和 return 通道；
- 初始化中途失败时关闭已经创建的 Channel，避免资源泄漏。

```yaml
rabbitmq:
  publisher:
    pool_size: 8
```

单元测试验证三个请求可以同时进入三个独立 slot；真实 RabbitMQ 集成测试使用三个 Channel 并发发布 30 条持久化消息，并确认所有消息均获得 Broker Confirm 且成功路由。

### 验证

`v1.1.4` 部署后，在相同 2C4G 单机环境中：

| 并发 | 指标 | 优化前 | 优化后 | 变化 |
|---:|---|---:|---:|---:|
| 10 | QPS | 101.18 | 165.52 | +63.6% |
| 10 | 平均延迟 | 98.653ms | 60.337ms | -38.8% |
| 10 | P95 | 186.689ms | 108.670ms | -41.8% |
| 25 | QPS | 115.90 | 190.72 | +64.6% |
| 25 | 平均延迟 | 215.517ms | 130.705ms | -39.4% |
| 25 | P95 | 345.730ms | 210.286ms | -39.2% |

两轮优化后测试的业务成功率均为 100%。并发 25 的优化后测试为 30 秒短时测试，优化前为 2 分钟测试，因此该组数据主要用于观察 Publisher 瓶颈解除后的趋势，不作为严格的同条件基准。

压测期间 API、MySQL 和 RabbitMQ CPU 分别约为 `44%`、`71%` 和 `73%`，说明请求已经从等待全局锁转为真实并行执行。`draw_result_queue` 在入口流量高于异步落库速度时仍会短暂积压，但停止压测后可以较快清空。

期间观察到 `task` 表一次批量更新 362～378 行耗时约 238～306ms。该 SQL 按主键批量更新，且没有造成队列持续阻塞，因此暂不为了消除慢 SQL 日志而缩小批次，避免增加数据库往返和事务提交次数（之后测试找到最合适的临界点）。
