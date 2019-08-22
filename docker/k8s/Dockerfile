FROM vitess/base AS base

FROM debian:stretch-slim

# TODO: remove when https://github.com/vitessio/vitess/issues/3553 is fixed
RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install mysql-client -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT /vtdataroot
ENV VTTOP /vt/src/vitess.io/vitess

# Prepare directory structure.
RUN mkdir -p /vt && \
    mkdir -p /vt/bin && \
    mkdir -p /vt/config && \
    mkdir -p /vt/web && \
    mkdir -p /vtdataroot/tabletdata

# Copy CA certs for https calls
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy binaries
COPY --from=base /vt/bin/mysqlctld /vt/bin/
COPY --from=base /vt/bin/vtctld /vt/bin/
COPY --from=base /vt/bin/vtctl /vt/bin/
COPY --from=base /vt/bin/vtctlclient /vt/bin/
COPY --from=base /vt/bin/vtgate /vt/bin/
COPY --from=base /vt/bin/vttablet /vt/bin/
COPY --from=base /vt/bin/vtworker /vt/bin/
COPY --from=base /vt/bin/vtbackup /vt/bin/

# copy web admin files
COPY --from=base $VTTOP/web /vt/web/

# copy vitess config
COPY --from=base $VTTOP/config/init_db.sql /vt/config/

# my.cnf include files
COPY --from=base $VTTOP/config/mycnf /vt/config/mycnf

# add vitess user and add permissions
RUN groupadd -r --gid 2000 vitess && useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt;
