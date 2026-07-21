#!/usr/bin/env bash

# 遇到命令失败、未定义变量或管道中的任一错误时立即退出，并让 ERR trap 在函数中生效。
set -Eeuo pipefail

# 从位置参数读取目标镜像标签和部署目录，并允许通过环境变量调整健康检查策略。
readonly TARGET_TAG="${1:-}"
readonly DEPLOY_PATH="${2:-}"
readonly HEALTHCHECK_ATTEMPTS="${HEALTHCHECK_ATTEMPTS:-60}"
readonly HEALTHCHECK_INTERVAL_SECONDS="${HEALTHCHECK_INTERVAL_SECONDS:-5}"

# 镜像标签必须符合 v主版本.次版本.修订版本 的格式，例如 v1.2.3。
if [[ ! "${TARGET_TAG}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	echo "invalid image tag: ${TARGET_TAG}" >&2
	exit 2
fi

# 部署目录必须是一个已经存在的绝对路径。
if [[ -z "${DEPLOY_PATH}" || "${DEPLOY_PATH}" != /* || ! -d "${DEPLOY_PATH}" ]]; then
	echo "invalid deploy path: ${DEPLOY_PATH}" >&2
	exit 2
fi

# 重试次数必须为正整数，重试间隔必须为非负整数。
if [[ ! "${HEALTHCHECK_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] ||
	[[ ! "${HEALTHCHECK_INTERVAL_SECONDS}" =~ ^[0-9]+$ ]]; then
	echo "invalid health check retry configuration" >&2
	exit 2
fi

# 提前确认部署流程依赖的命令均已安装，避免部署到一半才失败。
for command_name in awk curl docker flock mktemp; do
	if ! command -v "${command_name}" >/dev/null 2>&1; then
		echo "required command is missing: ${command_name}" >&2
		exit 2
	fi
done

# 后续所有相对路径和 Compose 操作都以部署目录为工作目录。
cd "${DEPLOY_PATH}"

readonly ENV_FILE="${DEPLOY_PATH}/.env"
readonly COMPOSE_FILE="${DEPLOY_PATH}/compose.yaml"

# .env 会在部署时被修改，因此必须存在且可写。
if [[ ! -f "${ENV_FILE}" || ! -w "${ENV_FILE}" ]]; then
	echo ".env is missing or not writable: ${ENV_FILE}" >&2
	exit 2
fi

# Compose 配置文件必须存在且可读。
if [[ ! -f "${COMPOSE_FILE}" || ! -r "${COMPOSE_FILE}" ]]; then
	echo "compose file is missing or not readable: ${COMPOSE_FILE}" >&2
	exit 2
fi

# 使用非阻塞文件锁保证同一部署目录同一时间只运行一个部署任务。
exec 9>"${DEPLOY_PATH}/.deploy.lock"
if ! flock -n 9; then
	echo "another deployment is already running" >&2
	exit 3
fi

# 将公共的 Compose 参数保存为数组，避免重复拼接命令和参数展开问题。
readonly -a COMPOSE=(docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}")

# .env 中必须且只能存在一个 IMAGE_TAG，防止更新到错误的配置项。
image_tag_lines="$(awk -F= '$1 == "IMAGE_TAG" { count++ } END { print count+0 }' "${ENV_FILE}")"
if [[ "${image_tag_lines}" != "1" ]]; then
	echo ".env must contain exactly one IMAGE_TAG entry" >&2
	exit 2
fi

# 保存部署前的镜像标签，后续部署失败时用它进行回滚。
previous_tag="$(awk -F= '$1 == "IMAGE_TAG" { sub(/^[^=]*=/, ""); print }' "${ENV_FILE}")"
if [[ -z "${previous_tag}" ]]; then
	echo "current IMAGE_TAG is empty" >&2
	exit 2
fi

env_temp_file=""
rollback_needed=0

# 通过临时文件原子替换 .env 中的 IMAGE_TAG，并保留原文件权限。
write_image_tag() {
	local image_tag="$1"
	env_temp_file="$(mktemp "${DEPLOY_PATH}/.env.XXXXXX")"
	awk -v image_tag="${image_tag}" '
		$0 ~ /^IMAGE_TAG=/ { print "IMAGE_TAG=" image_tag; next }
		{ print }
	' "${ENV_FILE}" >"${env_temp_file}"
	chmod --reference="${ENV_FILE}" "${env_temp_file}"
	mv "${env_temp_file}" "${ENV_FILE}"
	env_temp_file=""
}

# 部署失败后恢复旧镜像标签，并重新启动旧版本的 API 和 Admin 服务。
rollback() {
	echo "deployment failed; rolling back to ${previous_tag}" >&2
	write_image_tag "${previous_tag}"
	if ! "${COMPOSE[@]}" up -d api admin; then
		echo "automatic rollback failed; manual intervention is required" >&2
		return 1
	fi
	echo "IMAGE_TAG restored to ${previous_tag}" >&2
}

# 脚本退出时清理残留临时文件；若新版本已经写入，则自动执行回滚。
on_exit() {
	local exit_code=$?
	trap - EXIT
	if [[ -n "${env_temp_file}" ]]; then
		rm -f "${env_temp_file}"
	fi
	if ((exit_code != 0 && rollback_needed == 1)); then
		rollback || true
	fi
	exit "${exit_code}"
}
trap on_exit EXIT

# 轮询 API 和 Admin 的存活、就绪端点，全部通过后才认为部署成功。
wait_until_ready() {
	local -a endpoints=(
		"http://127.0.0.1:8080/healthz"
		"http://127.0.0.1:8080/readyz"
		"http://127.0.0.1:8081/healthz"
		"http://127.0.0.1:8081/readyz"
	)

	local attempt endpoint all_ready
	# 每一轮依次访问全部端点；任一端点失败都会进入下一轮重试。
	for ((attempt = 1; attempt <= HEALTHCHECK_ATTEMPTS; attempt++)); do
		all_ready=1
		for endpoint in "${endpoints[@]}"; do
			if ! curl --fail --silent --show-error --max-time 3 "${endpoint}" >/dev/null 2>&1; then
				all_ready=0
			fi
		done
		if ((all_ready == 1)); then
			echo "all health checks passed"
			return 0
		fi
		echo "waiting for services to become ready (${attempt}/${HEALTHCHECK_ATTEMPTS})"
		sleep "${HEALTHCHECK_INTERVAL_SECONDS}"
	done

	# 超过最大重试次数后输出每个端点的响应，便于定位具体失败服务。
	echo "health checks did not pass before timeout" >&2
	for endpoint in "${endpoints[@]}"; do
		echo "--- ${endpoint}" >&2
		curl --silent --show-error --max-time 3 "${endpoint}" >&2 || true
		echo >&2
	done
	return 1
}

# 调用只读的 Admin 业务接口，确认 HTTP、应用层、Repository 和 MySQL 查询链路均可用。
run_business_smoke_test() {
	local endpoint="http://127.0.0.1:8081/admin/v1/strategy/query_raffle_award_list?strategy_id=100001"
	local response

	if ! response="$(curl \
		--fail \
		--silent \
		--show-error \
		--max-time 5 \
		--request POST \
		"${endpoint}")"; then
		echo "business smoke test request failed: ${endpoint}" >&2
		return 1
	fi

	# HTTP 接口始终返回 200，因此还必须检查业务码和预置奖品，避免把业务错误误判为成功。
	if [[ "${response}" != *'"code":0'* ]] ||
		[[ "${response}" != *'"award_id":101'* ]]; then
		echo "business smoke test returned unexpected response: ${response}" >&2
		return 1
	fi

	echo "business smoke test passed"
}

# 在修改线上配置前，先验证目标标签对应的 Compose 配置是否合法。
echo "validating Compose configuration for ${TARGET_TAG}"
IMAGE_TAG="${TARGET_TAG}" "${COMPOSE[@]}" config --quiet

# 在修改 .env 前准备本次启动所需的全部镜像，任何镜像失败都会保持旧标签不变。
# 基础设施镜像版本固定，仅在服务器缺失时拉取；API/Admin 每次拉取目标版本以确认制品存在。
echo "preparing infrastructure images"
IMAGE_TAG="${TARGET_TAG}" "${COMPOSE[@]}" pull --policy missing mysql redis rabbitmq

echo "pulling API and Admin images for ${TARGET_TAG}"
IMAGE_TAG="${TARGET_TAG}" "${COMPOSE[@]}" pull api admin

# 从这里开始发生失败时需要回滚到 previous_tag。
echo "updating IMAGE_TAG from ${previous_tag} to ${TARGET_TAG}"
rollback_needed=1
write_image_tag "${TARGET_TAG}"

# 使用新镜像标签重新创建并在后台启动两个服务。
echo "starting API and Admin"
"${COMPOSE[@]}" up -d api admin

# 健康检查失败时输出容器状态和最近日志，随后退出并由 EXIT trap 触发回滚。
if ! wait_until_ready; then
	"${COMPOSE[@]}" ps >&2 || true
	"${COMPOSE[@]}" logs --tail=100 api admin >&2 || true
	exit 1
fi

# 健康端点只能证明进程和依赖存活；再执行一次只读业务查询后才判定新版本可用。
if ! run_business_smoke_test; then
	"${COMPOSE[@]}" ps >&2 || true
	"${COMPOSE[@]}" logs --tail=100 admin >&2 || true
	exit 1
fi

# 新版本已通过全部检查，关闭回滚标记并输出最终容器状态。
rollback_needed=0
echo "deployment of ${TARGET_TAG} succeeded"
"${COMPOSE[@]}" ps api admin
