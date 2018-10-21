# This image is only meant to be built from within the build.sh script.
FROM debian:jessie

# Install dependencies
RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db \
 && echo 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.0/debian jessie main' > /etc/apt/sources.list.d/mariadb.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bzip2 \
      mariadb-server \
 && rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTTOP /vt/src/vitess.io/vitess
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
USER vitess