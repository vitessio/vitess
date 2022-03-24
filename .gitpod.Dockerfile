FROM gitpod/workspace-full

USER gitpod

ENV HOME=/home/gitpod
WORKDIR $HOME

# Dazzle does not rebuild a layer until one of its lines are changed. Increase this counter to rebuild this layer.
ENV TRIGGER_REBUILD=1
ENV GO_VERSION=1.18
ENV GOPATH=$HOME/go-packages
ENV GOROOT=$HOME/go
ENV PATH=$GOROOT/bin:$GOPATH/bin:$PATH
run sudo rm -rf $GOROOT $GOPATH
RUN curl -fsSL https://dl.google.com/go/go$GO_VERSION.linux-amd64.tar.gz | tar xzs && \
# install VS Code Go tools for use with gopls as per https://github.com/golang/vscode-go/blob/master/docs/tools.md
# also https://github.com/golang/vscode-go/blob/27bbf42a1523cadb19fad21e0f9d7c316b625684/src/goTools.ts#L139
    go install github.com/uudashr/gopkgs/cmd/gopkgs@v2 && \
    go install github.com/ramya-rao-a/go-outline@latest && \
    go install github.com/cweill/gotests/gotests@latest && \
    go install github.com/fatih/gomodifytags@latest && \
    go install github.com/josharian/impl@latest && \
    go install github.com/haya14busa/goplay/cmd/goplay@latest && \
    go install github.com/go-delve/delve/cmd/dlv@latest && \
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest && \
    go install golang.org/x/tools/gopls@v0.7.4 && \
    go install golang.org/x/tools/cmd/goimports@latest && \
    sudo rm -rf $GOPATH/src $GOPATH/pkg /home/gitpod/.cache/go /home/gitpod/.cache/go-build

RUN sudo apt update -y \
    && sudo apt install -y ant maven default-jdk zip mysql-server mysql-client make unzip g++ etcd curl git wget \
    && sudo service mysql stop \
    && sudo service etcd stop \
    && sudo systemctl disable mysql \
    && sudo systemctl disable etcd

ENV PATH=~/vitess/bin:${PATH}
ENV VTDATAROOT=/tmp/vtdataroot
