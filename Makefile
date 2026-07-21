GO ?= go
DOCKER ?= docker
COMPOSE ?= docker compose
INTEGRATION_COMPOSE ?= $(COMPOSE) -f compose.integration.yaml
INTEGRATION_MYSQL_PASSWORD ?= prizeforge-integration
INTEGRATION_MYSQL_PORT ?= 13306
INTEGRATION_REDIS_PORT ?= 16379
export INTEGRATION_MYSQL_PASSWORD
export INTEGRATION_MYSQL_PORT
export INTEGRATION_REDIS_PORT

BIN_DIR ?= bin
IMAGE_PREFIX ?= prizeforge
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || printf 'dev')
LDFLAGS ?= -s -w

.DEFAULT_GOAL := help

.PHONY: help
help: ## 显示可用命令
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## 格式化 Go 代码
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## 检查是否存在未格式化的 Go 文件
	@files="$$(gofmt -l $$(find . -type f -name '*.go' -not -path './vendor/*'))"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' "以下文件需要执行 gofmt:" "$$files"; \
		exit 1; \
	fi

.PHONY: vet
vet: ## 运行 Go 静态检查
	$(GO) vet ./...

.PHONY: test
test: ## 运行全部测试
	$(GO) test ./...

.PHONY: test-race
test-race: ## 使用竞态检测运行全部测试
	$(GO) test -race ./...

.PHONY: test-cover
test-cover: ## 生成测试覆盖率报告 coverage.html
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	$(GO) tool cover -func=coverage.out

.PHONY: integration-up
integration-up: ## 启动并等待临时 MySQL、Redis 集成测试环境就绪
	$(INTEGRATION_COMPOSE) up -d --wait mysql redis

.PHONY: integration-db-check
integration-db-check: ## 验证集成测试分库和返利表已经初始化
	@db_count="$$( $(INTEGRATION_COMPOSE) exec -T mysql sh -ec 'mysql -uroot -p"$$MYSQL_ROOT_PASSWORD" -Nse "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('"'"'prizeforge'"'"', '"'"'prizeforge_01'"'"', '"'"'prizeforge_02'"'"')"' )"; \
	rebate_config_count="$$( $(INTEGRATION_COMPOSE) exec -T mysql sh -ec 'mysql -uroot -p"$$MYSQL_ROOT_PASSWORD" -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '"'"'prizeforge'"'"' AND table_name = '"'"'daily_behavior_rebate'"'"'"' )"; \
	rebate_order_table_count="$$( $(INTEGRATION_COMPOSE) exec -T mysql sh -ec 'mysql -uroot -p"$$MYSQL_ROOT_PASSWORD" -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('"'"'prizeforge_01'"'"', '"'"'prizeforge_02'"'"') AND table_name LIKE '"'"'user_behavior_rebate_order_%'"'"'"' )"; \
	test "$$db_count" = "3"; \
	test "$$rebate_config_count" = "1"; \
	test "$$rebate_order_table_count" = "8"; \
	printf '%s\n' "integration MySQL schema is ready"

.PHONY: integration-redis-check
integration-redis-check: ## 验证集成测试 Redis 已经就绪
	@response="$$( $(INTEGRATION_COMPOSE) exec -T redis redis-cli ping )"; \
	test "$$response" = "PONG"; \
	printf '%s\n' "integration Redis is ready"

.PHONY: integration-down
integration-down: ## 销毁临时集成测试环境及其数据
	$(INTEGRATION_COMPOSE) down --volumes --remove-orphans

.PHONY: integration-test
integration-test: ## 启动临时 MySQL、Redis，运行集成测试并自动销毁环境
	@set -eu; \
	INTEGRATION_MYSQL_PASSWORD='$(INTEGRATION_MYSQL_PASSWORD)' INTEGRATION_MYSQL_PORT='$(INTEGRATION_MYSQL_PORT)' INTEGRATION_REDIS_PORT='$(INTEGRATION_REDIS_PORT)' $(INTEGRATION_COMPOSE) up -d --wait mysql redis; \
	trap '$(INTEGRATION_COMPOSE) down --volumes --remove-orphans' EXIT; \
	$(MAKE) integration-db-check; \
	$(MAKE) integration-redis-check; \
	PRIZEFORGE_INTEGRATION_MYSQL_DSN='root:$(INTEGRATION_MYSQL_PASSWORD)@tcp(127.0.0.1:$(INTEGRATION_MYSQL_PORT))/prizeforge%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s' \
	PRIZEFORGE_INTEGRATION_REDIS_ADDR='127.0.0.1:$(INTEGRATION_REDIS_PORT)' \
		$(GO) test -tags=integration ./tests/integration/... -count=1

.PHONY: check
check: fmt-check vet test test-deploy ## 执行格式、静态检查和测试

.PHONY: test-deploy
test-deploy: ## 测试生产部署脚本的发布与回滚流程
	bash ./deploy/deploy_test.sh

.PHONY: build
build: build-api build-admin build-cdc ## 构建全部服务

.PHONY: build-api
build-api: ## 构建 API 服务
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/prizeforge-api ./cmd/api

.PHONY: build-admin
build-admin: ## 构建 Admin 服务
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/prizeforge-admin ./cmd/admin

.PHONY: build-cdc
build-cdc: ## 构建 CDC Sync 服务
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/prizeforge-cdc-sync ./cmd/cdc-sync

.PHONY: run-api
run-api: ## 本地启动 API 服务
	$(GO) run ./cmd/api

.PHONY: run-admin
run-admin: ## 本地启动 Admin 服务
	$(GO) run ./cmd/admin

.PHONY: run-cdc
run-cdc: ## 本地启动 CDC Sync 服务（配置来自 CDC_* 环境变量）
	$(GO) run ./cmd/cdc-sync

.PHONY: docker-build
docker-build: docker-build-api docker-build-admin docker-build-cdc ## 构建全部 Docker 镜像

.PHONY: docker-build-api
docker-build-api: ## 构建 API Docker 镜像
	$(DOCKER) build --target api -t $(IMAGE_PREFIX)-api:$(VERSION) .

.PHONY: docker-build-admin
docker-build-admin: ## 构建 Admin Docker 镜像
	$(DOCKER) build --target admin -t $(IMAGE_PREFIX)-admin:$(VERSION) .

.PHONY: docker-build-cdc
docker-build-cdc: ## 构建 CDC Sync Docker 镜像
	$(DOCKER) build --target cdc-sync -t $(IMAGE_PREFIX)-cdc-sync:$(VERSION) .

.PHONY: compose-config
compose-config: ## 校验并渲染 Compose 配置
	$(COMPOSE) config

.PHONY: infra-up
infra-up: ## 启动 MySQL、Redis 和 RabbitMQ
	$(COMPOSE) up -d mysql redis rabbitmq

.PHONY: monitoring-up
monitoring-up: ## 启动 Prometheus、Grafana 和 MySQL Exporter
	$(COMPOSE) up -d prometheus grafana mysql-exporter

.PHONY: search-up
search-up: ## 启动 Elasticsearch、Kibana 和 CDC Sync
	$(COMPOSE) up -d elasticsearch kibana cdc-sync

.PHONY: down
down: ## 停止 Compose 服务
	$(COMPOSE) down

.PHONY: logs
logs: ## 跟踪 Compose 日志
	$(COMPOSE) logs --tail=200 -f

.PHONY: clean
clean: ## 清理本地构建产物和覆盖率报告
	rm -rf $(BIN_DIR) coverage.out coverage.html
