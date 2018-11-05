ARG GIT_COMMIT

FROM square-vitess-build-${GIT_COMMIT} as build

FROM alpine:3.8

# Install some dependencies
RUN apk add bzip2 bash file

# Setup the Vitess environment
ENV VTTOP /vt/src/vitess.io/vitess
ENV VTROOT /vt
ENV GOTOP $VTTOP/go
ENV VTDATAROOT $VTROOT/vtdataroot
ENV GOBIN $VTROOT/bin
ENV GOPATH $VTROOT
ENV PATH $VTROOT/bin:$PATH
ENV VT_MYSQL_ROOT /usr
ENV PKG_CONFIG_PATH $VTROOT/lib

# Create the vitess user
RUN addgroup -S vitess && adduser -S -G vitess vitess && mkdir -p /vt && chown -R vitess:vitess /vt
USER vitess

# Make directories and copy binaries over from the vitess build image
RUN mkdir -p /vt/src/vitess.io/vitess/web/vtctld2 && mkdir -p /vt/vtdataroot/ && mkdir -p /vt/bin
COPY --from=build /vt/src/vitess.io/vitess/config /vt/config
COPY --from=build /vt/src/vitess.io/vitess/web/vtctld /vt/src/vitess.io/web/
COPY --from=build /vt/src/vitess.io/vitess/web/vtctld2/app /vt/src/vitess.io/web/vtctld2/
COPY --from=build /vt/bin/automation_client /vt/bin/automation_server /vt/bin/demo /vt/bin/mysqlctl /vt/bin/mysqlctld /vt/bin/query_analyzer /vt/bin/topo2topo /vt/bin/vtaclcheck /vt/bin/vtbench /vt/bin/vtclient /vt/bin/vtcombo /vt/bin/vtcopymissingrows /vt/bin/vtctl /vt/bin/vtctlclient /vt/bin/vtctld /vt/bin/vtexplain /vt/bin/vtgate /vt/bin/vtgateclienttest /vt/bin/vtqueryserver /vt/bin/vtrepairtable /vt/bin/vttablet /vt/bin/


