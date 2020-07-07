# We are using the public "planetscale-vitess" registry for this
NAME   := "us.gcr.io/planetscale-vitess/are-you-alive"
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
