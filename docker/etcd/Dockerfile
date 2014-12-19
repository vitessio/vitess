# This is a Dockerfile for etcd that is built on the same base image
# as the Vitess Dockerfile, so the base image can be shared.
#
# This image also contains bash, which is needed for startup scripts,
# such as those found in the Vitess Kubernetes example. The official
# etcd Docker image on quay.io doesn't have any shell in it.
FROM golang:1.3-wheezy

RUN mkdir -p src/github.com/coreos && \
    cd src/github.com/coreos && \
    curl -sL https://github.com/coreos/etcd/archive/v0.4.6.tar.gz | tar -xz && \
    mv etcd-0.4.6 etcd && \
    go install github.com/coreos/etcd
CMD ["etcd"]
