# We are using the "dev" project in our registry for this
NAME   := "us.gcr.io/planetscale-dev/are-you-alive"
TAG    := $$(git log -1 --pretty=%H)
IMG    := ${NAME}:${TAG}
LATEST := ${NAME}:latest

.PHONY: build push

build:
	@docker build -f build/release/Dockerfile -t ${IMG} .
	@docker tag ${IMG} ${LATEST}

push:
	@docker push ${IMG}
	@docker push ${LATEST}
