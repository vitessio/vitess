FROM golang:1.14-buster

# Install depedencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        unzip \
        wget

ARG PROTOC_GEN_GO_VERSION=v1.3.2

# Need to set up a module in order to install a specific version of protoc-gen-go.
#   1. The `go get package@version` syntax is only supported in module mode.
#   2. In module mode, `go get` is unsupported outside of a module.
# These two things together force us to do this quick hack.
#
# See https://github.com/golang/go/issues/24250 as just one of many issues on the subject.
RUN mkdir proto-builder && \
    cd proto-builder && \
    go mod init proto-builder && \
    GO111MODULE=on go get \
        golang.org/x/tools/cmd/goimports \
        github.com/golang/protobuf/protoc-gen-go@${PROTOC_GEN_GO_VERSION}

ARG PROTOC_VERSION=3.12.4

# Download protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /protoc && \
    mv /protoc/bin/protoc /usr/local/bin/ && \
    rm -rf protoc-${PROTOC_VERSION}-linux-x86_64.zip

ADD proto proto

# Build proto/$x.proto files into /go/vt/proto/$x/$x.pb.go
CMD [ "./proto/build.sh" ]
