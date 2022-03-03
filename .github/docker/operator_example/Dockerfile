FROM golang:1.17-alpine

WORKDIR /vitess

RUN apk add --no-cache build-base bash

COPY . .

RUN make build

FROM ubuntu:18.04

WORKDIR /vitess

RUN apt-get update
RUN apt-get install -y ca-certificates curl gnupg lsb-release make eatmydata mysql-client wget

# Install Go
RUN wget https://dl.google.com/go/go1.17.7.linux-amd64.tar.gz
RUN tar -xvf go1.17.7.linux-amd64.tar.gz
RUN mv go /usr/local

# Install Docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io

# Install kubectl
RUN apt-get install -y apt-transport-https ca-certificates curl
RUN curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update
RUN apt-get install -y kubectl

# Install kind
RUN curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.11.1/kind-linux-amd64
RUN chmod +x ./kind
RUN mv ./kind /usr/bin/kind

COPY --from=0 /vitess .

CMD sleep 3000