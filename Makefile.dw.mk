.PHONY: dwbuild-percona80
dwbuild-percona80:
	@GITHASH=$$(git rev-parse HEAD | head -c10) && \
	BOOTSTRAP_VERSION=$$(cat Makefile | grep BOOTSTRAP_VERSION | head -n1 | cut -d '=' -f2) && \
	VITESS_VERSION=$$(cat go/vt/servenv/version.go | grep versionName | cut -d '"' -f2) && \
	MYSQL_FLAVOR_VERSION=percona80 && \
	echo -e "Running the following build command in 5 seconds... \ndocker buildx build \
		--platform linux/amd64 \
		-f docker/lite/Dockerfile.$${MYSQL_FLAVOR_VERSION} \
		-t docker.ihs.demonware.net/platform-dbtech/vitess-lite:$${VITESS_VERSION}-$${MYSQL_FLAVOR_VERSION}-$${GITHASH} \
		--build-arg bootstrap_version=$${BOOTSTRAP_VERSION} ." && \
	sleep 5 && \
	docker buildx build \
    --platform linux/amd64 \
    -f docker/lite/Dockerfile.$${MYSQL_FLAVOR_VERSION} \
    -t docker.ihs.demonware.net/platform-dbtech/vitess-lite:v$${VITESS_VERSION}-$${MYSQL_FLAVOR_VERSION}-$${GITHASH} \
    --build-arg bootstrap_version=$${BOOTSTRAP_VERSION} .
