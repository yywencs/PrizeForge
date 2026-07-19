GO ?= go
DOCKER ?= docker
COMPOSE ?= docker compose

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

.PHONY: check
check: fmt-check vet test ## 执行格式、静态检查和测试

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
