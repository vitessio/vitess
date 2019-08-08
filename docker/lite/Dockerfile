FROM vitess/base AS builder
FROM debian:stretch-slim AS staging

RUN mkdir -p /vt/vtdataroot/ \
    && mkdir -p /vt/bin \
    && mkdir -p /vt/src/vitess.io/vitess/web/vtctld2 \
    && groupadd -r vitess && useradd -r -g vitess vitess

COPY --from=builder /vt/src/vitess.io/vitess/web/vtctld /vt/src/vitess.io/vitess/web/vtctld
COPY --from=builder /vt/src/vitess.io/vitess/web/vtctld2/app /vt/src/vitess.io/vitess/web/vtctld2/app
COPY --from=builder /vt/src/vitess.io/vitess/config /vt/config
COPY --from=builder /vt/bin/mysqlctld /vt/bin/
COPY --from=builder /vt/bin/vtctld /vt/bin/
COPY --from=builder /vt/bin/vtctlclient /vt/bin/
COPY --from=builder /vt/bin/vtgate /vt/bin/
COPY --from=builder /vt/bin/vttablet /vt/bin/
COPY --from=builder /vt/bin/vtworker /vt/bin/
COPY --from=builder /vt/bin/vtbackup /vt/bin/

RUN chown -R vitess:vitess /vt

FROM debian:stretch-slim

# Install dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    gnupg dirmngr ca-certificates wget libdbd-mysql-perl rsync libaio1 libatomic1 libcurl3 libev4 \
 && for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com 8C718D3B5072E1F5 && break; done \
 && echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7' > /etc/apt/sources.list.d/mysql.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
      bzip2 \
      libmysqlclient20 \
      mysql-client \
      mysql-server \
 && wget https://www.percona.com/downloads/XtraBackup/Percona-XtraBackup-2.4.13/binary/debian/stretch/x86_64/percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb \
 && dpkg -i percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb \
 && rm -f percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r vitess && useradd -r -g vitess vitess

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
COPY --from=staging /vt/ /vt/

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
USER vitess
