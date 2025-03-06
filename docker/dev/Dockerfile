FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget curl gnupg debconf-utils lsb-release \
        make gcc g++ unzip eatmydata xz-utils git \
        etcd-client etcd-server \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C \
    && wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.33-1_all.deb \
    && echo mysql-apt-config mysql-apt-config/select-server select mysql-8.0 | debconf-set-selections \
    && DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_*_all.deb \
    && rm mysql-apt-config_*_all.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends mysql-server mysql-client \
    && rm -rf /var/lib/apt/lists/* \
    && service mysql stop || true \
    && service etcd stop || true \
    && mkdir -p /etc/apparmor.d/disable \
    && ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/mysqld \
    && (apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld || true)

RUN wget --no-check-certificate https://go.dev/dl/go1.24.0.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz \
    && rm go1.24.0.linux-amd64.tar.gz
ENV PATH=/usr/local/go/bin:$PATH

ENV GONOPROXY=cel.dev
ENV GONOSUMDB=cel.dev
ENV GOINSECURE=cel.dev

WORKDIR /vt/src/vitess.io/vitess
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make minimaltools

CMD ["bash"]
