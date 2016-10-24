# This image is only meant to be built from within the build.sh script.
FROM debian:jessie

# Install dependencies
RUN apt-key adv --keyserver keys.gnupg.net \
        --recv-keys 8507EFA5 && \
    echo 'deb http://repo.percona.com/apt jessie main' > /etc/apt/sources.list.d/mysql.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-5.6 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-5.6 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        percona-server-server-5.6 bzip2 && \
    rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTTOP /vt/src/github.com/youtube/vitess
ENV VTROOT /vt
ENV GOTOP $VTTOP/go
ENV VTDATAROOT $VTROOT/vtdataroot
ENV GOBIN $VTROOT/bin
ENV GOPATH $VTROOT
ENV PATH $VTROOT/bin:$PATH
ENV VT_MYSQL_ROOT /usr
ENV PKG_CONFIG_PATH $VTROOT/lib

# Copy binaries (placed by build.sh)
COPY lite/vt /vt

# Create vitess user
RUN groupadd -r vitess && useradd -r -g vitess vitess && \
    mkdir -p /vt/vtdataroot && chown -R vitess:vitess /vt

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
