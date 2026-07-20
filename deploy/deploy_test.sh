#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly DEPLOY_SCRIPT="${SCRIPT_DIR}/deploy.sh"
readonly TEST_ROOT="$(mktemp -d)"

cleanup() {
	rm -rf "${TEST_ROOT}"
}
trap cleanup EXIT

docker() {
	printf '%s\n' "$*" >>"${TEST_DOCKER_LOG}"
	if [[ -n "${TEST_DOCKER_FAIL_ON}" && "$*" == *"${TEST_DOCKER_FAIL_ON}"* ]]; then
		return 1
	fi
}

curl() {
	[[ "${TEST_HEALTH_RESULT}" == "success" ]]
}

export -f docker curl
export TEST_DOCKER_LOG TEST_DOCKER_FAIL_ON TEST_HEALTH_RESULT

new_fixture() {
	local fixture_name="$1"
	local fixture_path="${TEST_ROOT}/${fixture_name}"
	mkdir -p "${fixture_path}"
	printf '%s\n' \
		'ACR_REGISTRY=registry.example.com' \
		'ACR_NAMESPACE=prizeforge' \
		'IMAGE_TAG=v1.0.0' \
		>"${fixture_path}/.env"
	printf '%s\n' 'services: {}' >"${fixture_path}/compose.yaml"
	printf '%s\n' "${fixture_path}"
}

assert_image_tag() {
	local fixture_path="$1"
	local expected_tag="$2"
	if ! grep -qx "IMAGE_TAG=${expected_tag}" "${fixture_path}/.env"; then
		echo "IMAGE_TAG was not ${expected_tag}" >&2
		exit 1
	fi
}

test_successful_deployment() {
	local fixture_path
	fixture_path="$(new_fixture success)"
	TEST_DOCKER_LOG="${fixture_path}/docker.log"
	TEST_DOCKER_FAIL_ON=""
	TEST_HEALTH_RESULT=success
	export TEST_DOCKER_LOG TEST_DOCKER_FAIL_ON TEST_HEALTH_RESULT

	HEALTHCHECK_ATTEMPTS=1 HEALTHCHECK_INTERVAL_SECONDS=0 \
		"${DEPLOY_SCRIPT}" v1.0.1 "${fixture_path}"

	assert_image_tag "${fixture_path}" v1.0.1
	grep -q 'pull --policy missing mysql redis rabbitmq' "${TEST_DOCKER_LOG}"
	grep -q 'pull api admin' "${TEST_DOCKER_LOG}"
	grep -q 'up -d api admin' "${TEST_DOCKER_LOG}"
}

test_pull_failure_keeps_previous_tag() {
	local case_spec case_name failure_pattern fixture_path
	for case_spec in \
		'infrastructure|pull --policy missing mysql redis rabbitmq' \
		'applications|pull api admin'; do
		IFS='|' read -r case_name failure_pattern <<<"${case_spec}"
		fixture_path="$(new_fixture "pull-failure-${case_name}")"
		TEST_DOCKER_LOG="${fixture_path}/docker.log"
		TEST_DOCKER_FAIL_ON="${failure_pattern}"
		TEST_HEALTH_RESULT=success
		export TEST_DOCKER_LOG TEST_DOCKER_FAIL_ON TEST_HEALTH_RESULT

		if HEALTHCHECK_ATTEMPTS=1 HEALTHCHECK_INTERVAL_SECONDS=0 \
			"${DEPLOY_SCRIPT}" v1.0.1 "${fixture_path}"; then
			echo "deployment unexpectedly succeeded after ${case_name} pull failure" >&2
			exit 1
		fi

		assert_image_tag "${fixture_path}" v1.0.0
		if grep -q 'up -d api admin' "${TEST_DOCKER_LOG}"; then
			echo "services were started after ${case_name} pull failure" >&2
			exit 1
		fi
	done
}

test_failed_deployment_rolls_back() {
	local fixture_path
	fixture_path="$(new_fixture rollback)"
	TEST_DOCKER_LOG="${fixture_path}/docker.log"
	TEST_DOCKER_FAIL_ON=""
	TEST_HEALTH_RESULT=failure
	export TEST_DOCKER_LOG TEST_DOCKER_FAIL_ON TEST_HEALTH_RESULT

	if HEALTHCHECK_ATTEMPTS=1 HEALTHCHECK_INTERVAL_SECONDS=0 \
		"${DEPLOY_SCRIPT}" v1.0.1 "${fixture_path}"; then
		echo "deployment unexpectedly succeeded" >&2
		exit 1
	fi

	assert_image_tag "${fixture_path}" v1.0.0
	if [[ "$(grep -c 'up -d api admin' "${TEST_DOCKER_LOG}")" != "2" ]]; then
		echo "rollback did not restart the previous image tag" >&2
		exit 1
	fi
}

test_invalid_tag_is_rejected() {
	local fixture_path
	fixture_path="$(new_fixture invalid-tag)"
	TEST_DOCKER_LOG="${fixture_path}/docker.log"
	TEST_DOCKER_FAIL_ON=""
	TEST_HEALTH_RESULT=success
	export TEST_DOCKER_LOG TEST_DOCKER_FAIL_ON TEST_HEALTH_RESULT

	if "${DEPLOY_SCRIPT}" latest "${fixture_path}"; then
		echo "invalid image tag was accepted" >&2
		exit 1
	fi
	assert_image_tag "${fixture_path}" v1.0.0
}

test_successful_deployment
test_pull_failure_keeps_previous_tag
test_failed_deployment_rolls_back
test_invalid_tag_is_rejected

echo "deploy script tests passed"
