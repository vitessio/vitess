# This image is only meant to be built from within the build.sh script.
FROM debian:jessie

# Install dependencies
RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 5072E1F5 \
 && echo 'deb http://repo.mysql.com/apt/debian/ jessie mysql-5.7' > /etc/apt/sources.list.d/mysql.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
      bzip2 \
      libmysqlclient18 \
      mysql-client \
      mysql-server \
 && rm -rf /var/lib/apt/lists/*

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
