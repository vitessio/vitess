# This is a Dockerfile for etcd that is built on the same base image
# as the Vitess Dockerfile, so the base image can be shared.
#
# This image also contains bash, which is needed for startup scripts,
# such as those found in the Vitess Kubernetes example. The official
# etcd Docker image on quay.io doesn't have any shell in it.
FROM golang:1.5

ADD getsrv.go /go/
RUN GOBIN=/go/bin go install /go/getsrv.go

RUN mkdir -p src/github.com/coreos && \
    cd src/github.com/coreos && \
    curl -sL https://github.com/coreos/etcd/archive/v2.0.13.tar.gz | tar -xz && \
    mv etcd-2.0.13 etcd && \
    go install github.com/coreos/etcd github.com/coreos/etcd/etcdctl
CMD ["etcd"]
