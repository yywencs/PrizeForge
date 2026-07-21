# PrizeForge 抽奖接口压测工具

该工具用于压测完整抽奖接口：

```text
POST /api/v1/raffle/activity/draw
```

提供两个子命令：

- `prepare`：准备压测用户、抽奖额度、活动状态、奖品库存和 Redis 缓存。
- `run`：并发请求抽奖接口，统计 QPS、成功率和延迟。

## 构建与帮助

编译本地二进制：

```bash
go build -o ./bin/prizeforge-benchmark ./cmd/benchmark
```

构建 Docker 镜像（命令需要在仓库根目录执行）：

```bash
docker build \
  --file cmd/benchmark/Dockerfile \
  --tag prizeforge-benchmark:local \
  .
```

查看帮助：

```bash
./bin/prizeforge-benchmark prepare --help
./bin/prizeforge-benchmark run --help

docker run --rm prizeforge-benchmark:local prepare --help
docker run --rm prizeforge-benchmark:local run --help
```

## 准备数据

`prepare` 会修改所连接的 MySQL 和 Redis 数据。MySQL DSN 中必须保留 `%s`，程序会依次连接 `prizeforge`、`prizeforge_01` 和 `prizeforge_02`。

先设置连接信息：

```bash
export PRIZEFORGE_BENCHMARK_MYSQL_DSN='root:你的MySQL密码@tcp(mysql:3306)/prizeforge%s?charset=utf8mb4&parseTime=true&loc=Local&timeout=10s'
export PRIZEFORGE_BENCHMARK_REDIS_ADDR='redis:6379'
export PRIZEFORGE_BENCHMARK_REDIS_PASSWORD='你的Redis密码'
```

使用 Docker Compose 内部网络执行：

```bash
docker run --rm \
  --network prizeforge_backend \
  --env PRIZEFORGE_BENCHMARK_MYSQL_DSN \
  --env PRIZEFORGE_BENCHMARK_REDIS_ADDR \
  --env PRIZEFORGE_BENCHMARK_REDIS_PASSWORD \
  prizeforge-benchmark:local \
  prepare \
  --confirm-reset \
  --admin-url http://admin:8081 \
  --activity-id 100301 \
  --users 1000 \
  --quota 20 \
  --user-prefix benchmark-0721
```

本地运行时，可以将镜像命令替换为：

```bash
./bin/prizeforge-benchmark prepare \
  --confirm-reset \
  --admin-url http://127.0.0.1:8081 \
  --activity-id 100301 \
  --users 1000 \
  --quota 20 \
  --user-prefix benchmark-0721
```

生成的用户 ID 格式为 `benchmark-0721-000001`。`prepare` 可以重复执行，但不会删除已经产生的订单、中奖记录和任务数据。

## 运行压测

`activity-id`、`users` 和 `user-prefix` 必须与 `prepare` 保持一致：

```bash
docker run --rm \
  --network prizeforge_backend \
  prizeforge-benchmark:local \
  run \
  --url http://api:8080 \
  --activity-id 100301 \
  --users 1000 \
  --user-prefix benchmark-0721 \
  --concurrency 10 \
  --duration 30s \
  --timeout 5s
```

本地二进制的运行方式：

```bash
./bin/prizeforge-benchmark run \
  --url http://127.0.0.1:8080 \
  --activity-id 100301 \
  --users 1000 \
  --user-prefix benchmark-0721 \
  --concurrency 10 \
  --duration 30s \
  --timeout 5s
```

程序会循环选择压测用户，并为每个请求生成唯一的 `request_id`。按 `Ctrl+C` 可以提前结束并输出结果。

## 参数说明

### prepare

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--mysql-dsn` | 环境变量 | MySQL DSN 模板，必须包含一个 `%s` |
| `--redis-addr` | `127.0.0.1:6379` | Redis 地址 |
| `--redis-password` | 环境变量 | Redis 密码 |
| `--redis-db` | `0` | Redis DB |
| `--admin-url` | `http://127.0.0.1:8081` | Admin 服务根地址 |
| `--activity-id` | `100301` | 要准备的活动 ID |
| `--users` | `1000` | 压测用户数量 |
| `--quota` | `20` | 每个用户的总、日、月抽奖额度 |
| `--user-prefix` | `benchmark-user` | 压测用户 ID 前缀 |
| `--db-count` | `2` | 分库数量，必须与服务配置一致 |
| `--table-count` | `4` | 每个分库的分表数量，必须与服务配置一致 |
| `--batch-size` | `500` | 每批写入的用户数量 |
| `--timeout` | `2m` | 整个数据准备过程的超时时间 |
| `--skip-armory` | `false` | 跳过 Admin 策略装配调用 |
| `--confirm-reset` | `false` | 确认修改数据，执行 `prepare` 时必须指定 |

### run

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--url` | `http://127.0.0.1:8080` | API 服务根地址，不包含接口路径 |
| `--activity-id` | `100301` | 被压测的活动 ID |
| `--users` | `1000` | 轮转使用的用户数量 |
| `--user-prefix` | `benchmark-user` | 压测用户 ID 前缀 |
| `--concurrency` | `10` | 并发工作协程数量 |
| `--duration` | `30s` | 压测持续时间 |
| `--timeout` | `5s` | 单个 HTTP 请求超时时间 |

## 结果说明

| 字段 | 含义 |
| --- | --- |
| `total requests` | 完成的请求总数 |
| `requests/sec` | 平均每秒完成的请求数，即 QPS |
| `business success` | 响应业务码为 `code=0` 的请求 |
| `business errors` | HTTP 请求成功，但业务码非零的请求 |
| `transport errors` | 连接失败、超时或连接重置等网络错误 |
| `HTTP errors` | HTTP 状态码不是 2xx 的请求 |
| `decode errors` | 响应无法按预期 JSON 解析的请求 |
| `latency avg` | 平均请求延迟 |
| `latency p50/p95/p99` | 对应百分位请求延迟 |
| `latency max` | 最大请求延迟 |

PrizeForge 的业务错误也可能返回 HTTP 200，因此需要同时检查 `business success`、`business errors` 和 `business codes`。
