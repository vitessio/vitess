FROM vitess/base AS base
FROM debian:stretch-slim
COPY --from=base /vt/bin/vtctlclient /usr/bin/
CMD ["/usr/bin/vtctlclient"]
