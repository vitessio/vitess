FROM golang:1.14-buster

ARG PROTOC_VERSION=3.12.4

# Install depedencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        unzip \
        wget && \
    go get \
        golang.org/x/tools/cmd/goimports \
        github.com/golang/protobuf/protoc-gen-go && \
    go install \
        golang.org/x/tools/cmd/goimports \
        github.com/golang/protobuf/protoc-gen-go

# Download protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /protoc && \
    mv /protoc/bin/protoc /usr/local/bin/ && \
    rm -rf protoc-${PROTOC_VERSION}-linux-x86_64.zip

ADD proto proto

# Build proto/$x.proto files into /go/vt/proto/$x/$x.pb.go
CMD [ "./proto/build.sh" ]
