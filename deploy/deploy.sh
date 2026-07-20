#!/usr/bin/env bash

set -Eeuo pipefail

readonly TARGET_TAG="${1:-}"
readonly DEPLOY_PATH="${2:-}"
readonly HEALTHCHECK_ATTEMPTS="${HEALTHCHECK_ATTEMPTS:-60}"
readonly HEALTHCHECK_INTERVAL_SECONDS="${HEALTHCHECK_INTERVAL_SECONDS:-5}"

if [[ ! "${TARGET_TAG}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	echo "invalid image tag: ${TARGET_TAG}" >&2
	exit 2
fi

if [[ -z "${DEPLOY_PATH}" || "${DEPLOY_PATH}" != /* || ! -d "${DEPLOY_PATH}" ]]; then
	echo "invalid deploy path: ${DEPLOY_PATH}" >&2
	exit 2
fi

if [[ ! "${HEALTHCHECK_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] ||
	[[ ! "${HEALTHCHECK_INTERVAL_SECONDS}" =~ ^[0-9]+$ ]]; then
	echo "invalid health check retry configuration" >&2
	exit 2
fi

for command_name in awk curl docker flock mktemp; do
	if ! command -v "${command_name}" >/dev/null 2>&1; then
		echo "required command is missing: ${command_name}" >&2
		exit 2
	fi
done

cd "${DEPLOY_PATH}"

readonly ENV_FILE="${DEPLOY_PATH}/.env"
readonly COMPOSE_FILE="${DEPLOY_PATH}/compose.yaml"

if [[ ! -f "${ENV_FILE}" || ! -w "${ENV_FILE}" ]]; then
	echo ".env is missing or not writable: ${ENV_FILE}" >&2
	exit 2
fi

if [[ ! -f "${COMPOSE_FILE}" || ! -r "${COMPOSE_FILE}" ]]; then
	echo "compose file is missing or not readable: ${COMPOSE_FILE}" >&2
	exit 2
fi

exec 9>"${DEPLOY_PATH}/.deploy.lock"
if ! flock -n 9; then
	echo "another deployment is already running" >&2
	exit 3
fi

readonly -a COMPOSE=(docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}")

image_tag_lines="$(awk -F= '$1 == "IMAGE_TAG" { count++ } END { print count+0 }' "${ENV_FILE}")"
if [[ "${image_tag_lines}" != "1" ]]; then
	echo ".env must contain exactly one IMAGE_TAG entry" >&2
	exit 2
fi

previous_tag="$(awk -F= '$1 == "IMAGE_TAG" { sub(/^[^=]*=/, ""); print }' "${ENV_FILE}")"
if [[ -z "${previous_tag}" ]]; then
	echo "current IMAGE_TAG is empty" >&2
	exit 2
fi

env_temp_file=""
rollback_needed=0

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

rollback() {
	echo "deployment failed; rolling back to ${previous_tag}" >&2
	write_image_tag "${previous_tag}"
	if ! "${COMPOSE[@]}" up -d api admin; then
		echo "automatic rollback failed; manual intervention is required" >&2
		return 1
	fi
	echo "IMAGE_TAG restored to ${previous_tag}" >&2
}

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

wait_until_ready() {
	local -a endpoints=(
		"http://127.0.0.1:8080/healthz"
		"http://127.0.0.1:8080/readyz"
		"http://127.0.0.1:8081/healthz"
		"http://127.0.0.1:8081/readyz"
	)

	local attempt endpoint all_ready
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

	echo "health checks did not pass before timeout" >&2
	for endpoint in "${endpoints[@]}"; do
		echo "--- ${endpoint}" >&2
		curl --silent --show-error --max-time 3 "${endpoint}" >&2 || true
		echo >&2
	done
	return 1
}

echo "validating Compose configuration for ${TARGET_TAG}"
IMAGE_TAG="${TARGET_TAG}" "${COMPOSE[@]}" config --quiet

echo "pulling API and Admin images for ${TARGET_TAG}"
IMAGE_TAG="${TARGET_TAG}" "${COMPOSE[@]}" pull api admin

echo "updating IMAGE_TAG from ${previous_tag} to ${TARGET_TAG}"
rollback_needed=1
write_image_tag "${TARGET_TAG}"

echo "starting API and Admin"
"${COMPOSE[@]}" up -d api admin

if ! wait_until_ready; then
	"${COMPOSE[@]}" ps >&2 || true
	"${COMPOSE[@]}" logs --tail=100 api admin >&2 || true
	exit 1
fi

rollback_needed=0
echo "deployment of ${TARGET_TAG} succeeded"
"${COMPOSE[@]}" ps api admin
