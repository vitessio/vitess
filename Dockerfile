FROM golang:1.3-wheezy

# Install Vitess build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    automake \
    bison \
    bzip2 \
    curl \
    g++ \
    git \
    libssl-dev \
    libtool \
    make \
    memcached \
    mercurial \
    openjdk-7-jre-headless \
    pkg-config \
    python-dev \
    python-mysqldb \
    python-software-properties \
    && rm -rf /var/lib/apt/lists/*

# Install MariaDB 10.0.x
RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db && \
    add-apt-repository 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.0/debian wheezy main' && \
    apt-get update && apt-get install -y mariadb-server libmariadbclient-dev

# Load files from directory containing Dockerfile
COPY . /vt/src/github.com/youtube/vitess

# Create vitess user
RUN groupadd -r vitess && useradd -r -g vitess vitess && \
    chown -R vitess:vitess /vt
USER vitess

# Bootstrap Vitess
WORKDIR /vt/src/github.com/youtube/vitess
ENV MYSQL_FLAVOR MariaDB
RUN ./bootstrap.sh

# Set up environment (equivalent to '. dev.env')
ENV VTTOP /vt/src/github.com/youtube/vitess
ENV VTROOT /vt
ENV GOTOP $VTTOP/go
ENV PYTOP $VTTOP/py
ENV VTDATAROOT $VTROOT/vtdataroot
ENV VTPORTSTART 15000
ENV PYTHONPATH $VTROOT/dist/py-cbson/lib/python2.7/site-packages:$VTROOT/dist/py-vt-bson-0.3.2/lib/python2.7/site-packages:$VTROOT/py-vtdb
ENV GOBIN $VTROOT/bin
ENV GOPATH $VTROOT
ENV PATH $VTROOT/bin:$PATH
ENV VT_MYSQL_ROOT /usr
ENV PKG_CONFIG_PATH $VTROOT/lib
ENV CGO_CFLAGS -I$VTROOT/dist/vt-zookeeper-3.3.5/include/c-client-src
ENV CGO_LDFLAGS -L$VTROOT/dist/vt-zookeeper-3.3.5/lib
ENV LD_LIBRARY_PATH $VTROOT/dist/vt-zookeeper-3.3.5/lib

# Build Vitess
RUN make build

# If the user doesn't specify a command, load a shell.
CMD ["/bin/bash"]
