FROM golang:1.9

# Install Vitess build dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    # TODO(mberlin): Group these to make it easier to understand which library actually requires them.
    automake \
    bison \
    bzip2 \
    chromium \
    curl \
    g++ \
    git \
    libgconf-2-4 \
    libtool \
    make \
    openjdk-8-jdk \
    pkg-config \
    python-crypto \
    python-dev \
    python-mysqldb \
    python-pip \
    ruby \
    ruby-dev \
    software-properties-common \
    virtualenv \
    unzip \
    xvfb \
    zip \
    libz-dev \
    && rm -rf /var/lib/apt/lists/*

# Not sure if this is needed, but it was part of php init, which we don't do any more.
RUN mkdir -p /vt/bin

# Install Maven 3.1+
RUN mkdir -p /vt/dist && \
    cd /vt/dist && \
    curl -sL --connect-timeout 10 --retry 3 \
        http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz | tar -xz && \
    mv apache-maven-3.3.9 maven

# Set up Vitess environment (equivalent to '. dev.env')
ENV VTTOP /vt/src/github.com/youtube/vitess
ENV VTROOT /vt
ENV GOTOP $VTTOP/go
ENV PYTOP $VTTOP/py
ENV VTDATAROOT $VTROOT/vtdataroot
ENV VTPORTSTART 15000
ENV PYTHONPATH $VTROOT/dist/py-mock-1.0.1/lib/python2.7/site-packages:$VTROOT/py-vtdb:$VTROOT/dist/selenium/lib/python2.7/site-packages
ENV GOBIN $VTROOT/bin
ENV GOPATH $VTROOT
ENV PATH $VTROOT/bin:$VTROOT/dist/maven/bin:$VTROOT/dist/chromedriver:$PATH
ENV VT_MYSQL_ROOT /usr
ENV PKG_CONFIG_PATH $VTROOT/lib

# Copy files needed for bootstrap
COPY bootstrap.sh dev.env /vt/src/github.com/youtube/vitess/
COPY config /vt/src/github.com/youtube/vitess/config
COPY third_party /vt/src/github.com/youtube/vitess/third_party
COPY tools /vt/src/github.com/youtube/vitess/tools
COPY travis /vt/src/github.com/youtube/vitess/travis
COPY vendor/vendor.json /vt/src/github.com/youtube/vitess/vendor/

# grpcio runtime is needed for python tests.
RUN grpcio_ver="1.7.0" && \
    pip install --upgrade grpcio==$grpcio_ver

# Download vendored Go dependencies
RUN cd /vt/src/github.com/youtube/vitess && \
    go get -u github.com/kardianos/govendor && \
    govendor sync && \
    rm -rf /vt/.cache

# Create vitess user
RUN groupadd -r vitess && useradd -r -g vitess vitess && \
    mkdir -p /vt/vtdataroot /home/vitess && \
    chown -R vitess:vitess /vt /home/vitess

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot

# If the user doesn't specify a command, load a shell.
CMD ["/bin/bash"]

