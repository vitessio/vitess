#!/bin/bash

# Smoke test for vttestserver docker images across architectures.
# Builds and tests each platform sequentially, cleaning up after each.
#
# Usage:
#   ./test.sh                          # test both arm64 and amd64 for mysql84
#   ./test.sh mysql80                  # test both arm64 and amd64 for mysql80
#   ./test.sh mysql84 linux/arm64      # test only arm64 for mysql84

set -euo pipefail

FLAVOR="${1:-mysql84}"
PLATFORMS="${2:-linux/arm64 linux/amd64}"
CONTAINER_NAME="vttestserver-smoke-test"
IMAGE_NAME="vttestserver-smoke:${FLAVOR}"
VTGATE_PORT=33574
VTCOMBO_MYSQL_PORT=33577
TIMEOUT=120

cd "$(git rev-parse --show-toplevel)"

cleanup() {
	docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

smoke_test() {
	local platform="$1"
	local arch="${platform#linux/}"
	echo "============================================"
	echo "Testing ${FLAVOR} on ${platform}"
	echo "============================================"

	echo "--- Building image ---"
	docker buildx build \
		--platform="${platform}" \
		-f "docker/vttestserver/Dockerfile.${FLAVOR}" \
		-t "${IMAGE_NAME}" \
		--load . 2>&1 | tail -5

	echo "--- Starting container ---"
	cleanup
	docker run -d \
		--name "${CONTAINER_NAME}" \
		--platform="${platform}" \
		-e PORT=${VTGATE_PORT} \
		-e KEYSPACES=test \
		-e NUM_SHARDS=1 \
		-e MYSQL_BIND_HOST=0.0.0.0 \
		-p ${VTGATE_PORT}:${VTGATE_PORT} \
		-p ${VTCOMBO_MYSQL_PORT}:${VTCOMBO_MYSQL_PORT} \
		"${IMAGE_NAME}" >/dev/null

	echo "--- Waiting for vttestserver to be ready (up to ${TIMEOUT}s) ---"
	for i in $(seq 1 $((TIMEOUT / 2))); do
		if docker logs "${CONTAINER_NAME}" 2>&1 | grep -q "vtcombo_mysql_port"; then
			echo "Ready after $((i * 2))s"
			break
		fi
		if [ "$i" -eq $((TIMEOUT / 2)) ]; then
			echo "FAIL: timed out waiting for vttestserver"
			docker logs "${CONTAINER_NAME}" 2>&1 | tail -20
			return 1
		fi
		sleep 2
	done

	echo "--- Running queries ---"
	mysql -h 127.0.0.1 -P ${VTCOMBO_MYSQL_PORT} -u root -e "
		SELECT VERSION();
		CREATE TABLE test.smoke_test (id INT PRIMARY KEY, val VARCHAR(50));
		INSERT INTO test.smoke_test VALUES (1, '${arch}-${FLAVOR}');
		SELECT * FROM test.smoke_test;
		SHOW VITESS_TABLETS;
		DROP TABLE test.smoke_test;
	"

	cleanup
	echo "PASS: ${FLAVOR} on ${platform}"
	echo ""
}

failures=0
for platform in ${PLATFORMS}; do
	if ! smoke_test "${platform}"; then
		failures=$((failures + 1))
	fi
done

if [ "${failures}" -gt 0 ]; then
	echo "FAIL: ${failures} platform(s) failed"
	exit 1
fi

echo "All smoke tests passed."
