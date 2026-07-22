# 逐任务库存同步压测基线（v1.0.3～v1.0.4）

## 概述

本文档记录 PrizeForge 在引入“按 `(strategy_id, award_id)` 聚合批量扣减库存”之前的完整抽奖链路性能。

这些数据最初只保留在压测终端输出中，没有及时形成独立文档。本记录根据保留的原始输出恢复，指标均按当时
输出原样抄录；没有重新部署旧版本复测。优化后的结果见
[Outbox 批量库存同步压测基线（v1.0.5）](./outbox_batch_stock_sync_baseline_2026-07-22.md)。

## 优化前代码行为

通过 `v1.0.4` Tag 的代码可以确认，当时的库存 Outbox 处理方式为：

1. 每 5 秒扫描各分库的 `task` 表；
2. 为扫描到的每条任务启动一个独立 goroutine；
3. 每条 `strategy_award_stock_sync` 任务单独调用 `UpdateStrategyAwardStock`；
4. 每个订单分别开启一个 MySQL 事务；
5. 事务中先插入一条 `strategy_award_stock_reservation` 幂等凭证；
6. 随后单独执行一次 `award_count_surplus = award_count_surplus - 1`；
7. 每条任务再单独更新一次 Outbox 状态。

可以使用以下命令查看当时的实现：

```bash
git show v1.0.4:internal/job/send_award_message.go
git show v1.0.4:internal/infrastructure/repository/strategyrepo/award.go
```

当大量用户抽中同一个奖品时，多笔事务会同时更新相同的 `strategy_award` 行，从而在该行上排队竞争锁。

## 测试条件

| 项目 | 配置 |
| --- | --- |
| 测试日期 | `2026-07-21` |
| 主要基线版本 | `v1.0.3` |
| 补充确认版本 | `v1.0.4`（仅将 GORM 日志调整为错误和慢 SQL，库存同步逻辑未改变） |
| 机器配置 | 单机 `2C4G` |
| 部署方式 | API、Admin、MySQL、Redis、RabbitMQ 均运行在同一台服务器的 Docker 中 |
| 压测工具 | `prizeforge-benchmark:v1.0.0` |
| 压测位置 | 被测服务器本机，Docker `host` 网络 |
| API 地址 | `http://223.109.143.131:8080`（服务器公网地址） |
| 接口 | `POST /api/v1/raffle/activity/draw` |
| 活动 ID | `100301` |
| 用户数量 | `10000` |
| 用户前缀 | `benchmark-formal-0721` |
| 单请求超时 | `5s` |

## v1.0.3 主要基线结果

| 并发数 | 持续时间 | 总请求数 | QPS | 成功率 | 平均延迟 | P50 | P95 | P99 | 最大延迟 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | `1m` | `3681` | `61.26` | `100%` | `163.114ms` | `155.463ms` | `247.658ms` | `290.374ms` | `419.300ms` |
| 25 | `2m` | `9746` | `81.04` | `100%` | `308.234ms` | `287.425ms` | `482.558ms` | `579.932ms` | `1.635324s` |
| 40 | `1m` | `4962` | `82.51` | `100%` | `484.283ms` | `465.676ms` | `748.786ms` | `868.472ms` | `1.131567s` |

三轮测试均满足：

- `business errors = 0`
- `transport errors = 0`
- `HTTP errors = 0`
- `decode errors = 0`

并发从 25 增加到 40 后，QPS 只从 `81.04` 增加到 `82.51`，平均延迟却从 `308ms` 增加到
`484ms`，说明系统在并发 25 左右已经进入吞吐平台。

## v1.0.4 补充确认结果

将 GORM 调整为只输出错误和慢 SQL 后，又进行了一轮并发 10、持续 1 分钟的确认测试：

| 并发数 | 持续时间 | 总请求数 | QPS | 成功率 | 平均延迟 | P50 | P95 | P99 | 最大延迟 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | `1m` | `3322` | `55.31` | `100%` | `180.712ms` | `170.232ms` | `276.273ms` | `374.702ms` | `483.363ms` |

该轮结果没有因为减少普通 SQL 日志而提高，说明 GORM 日志并不是主要瓶颈。当时数据库中已经存在较多
Outbox 积压，运行状态与主要基线并不完全一致，因此该结果只作为补充样本，不用于计算完整并发曲线。

## 资源使用快照

并发 25 测试期间的一次 `docker stats` 快照：

| 容器 | CPU | 内存使用 | 内存限制 |
| --- | ---: | ---: | ---: |
| API | `62.82%` | `12.92MiB` | `512MiB` |
| Admin | `0.00%` | `4.973MiB` | `256MiB` |
| Redis | `8.96%` | `4.785MiB` | `256MiB` |
| MySQL | `93.47%` | `548.1MiB` | `1GiB` |
| RabbitMQ | `0.34%` | `86.99MiB` | `512MiB` |

当时的 `vmstat` 样本显示 CPU 空闲率一度降至 `0%～5%`，运行队列明显超过两个 CPU 核心，系统已接近
CPU 饱和。RabbitMQ 队列保持 `messages_ready = 0`、`messages_unacknowledged = 0`，不是主要瓶颈。

## Outbox 状态快照

`v1.0.4` 确认测试后，两个分库的任务状态如下：

| 分库 | Topic | 状态 | 数量 |
| --- | --- | --- | ---: |
| `prizeforge_01` | `send_award` | `completed` | `2711` |
| `prizeforge_01` | `send_award` | `create` | `8237` |
| `prizeforge_01` | `strategy_award_stock_sync` | `completed` | `2666` |
| `prizeforge_01` | `strategy_award_stock_sync` | `create` | `8237` |
| `prizeforge_02` | `send_award` | `completed` | `2665` |
| `prizeforge_02` | `send_award` | `create` | `8234` |
| `prizeforge_02` | `strategy_award_stock_sync` | `completed` | `2665` |
| `prizeforge_02` | `strategy_award_stock_sync` | `create` | `8234` |

当时共有 `32942` 条 `create` 状态任务。Asynq 的 `scheduled` 和 `pending` 均为 0，RabbitMQ 队列也没有积压，
压力主要位于数据库 Outbox 扫描、库存同步和任务状态更新链路。

## 慢 SQL 证据

当时多次观察到以下 SQL 超过 `200ms`：

```sql
UPDATE strategy_award
SET award_count_surplus = award_count_surplus - 1,
    update_time = ?
WHERE strategy_id = ?
  AND award_id = ?
  AND award_count_surplus > 0;
```

保留样本中的耗时包括：

```text
201.859ms
211.194ms
226.373ms
231.537ms
245.881ms
309.557ms
394.765ms
```

这些 SQL 都更新相同策略奖品对应的单条库存记录，与旧代码“每条库存消息一个事务、一次库存 UPDATE”的行为
相吻合，是后续进行库存消息聚合和批量扣减的直接依据。

## 优化决策

针对上述现象，`v1.0.5` 采用以下改造：

1. 扫描后仅对 `strategy_award_stock_sync` 任务分组；
2. 分组键使用 `(strategy_id, award_id)`，与库存行的业务唯一条件一致；
3. 同组订单在一个事务中逐个写入幂等凭证；
4. 只统计本批首次出现的订单数量 `N`；
5. 对库存行执行一次 `award_count_surplus = award_count_surplus - N`；
6. 使用有界工作池，避免为每条任务创建无上限 goroutine。

## 适用范围与限制

- 本文档由保留的终端输出恢复，不是对旧版本的重新测试。
- `v1.0.3` 主要基线使用服务器公网地址，`v1.0.5` 使用 `127.0.0.1`；两者均从服务器本机发起，
  但网络路径并不完全相同。
- 三个主要档位的持续时间不完全一致，并发 40 的旧基线只有 1 分钟。
- 各轮测试之间的历史 Outbox 积压量不同，因此前后结果适合展示优化趋势，不属于严格控制全部变量的实验。
- 简历和项目说明中应使用“约提升 39%”或“在该测试条件下提升约 35%～39%”，避免将其描述为普适结论。
