FROM vitess/bootstrap:common

RUN apt-get update
RUN apt-get install -y sudo curl vim jq mysql-server

COPY docker/local/install_local_dependencies.sh /vt/dist/install_local_dependencies.sh
RUN /vt/dist/install_local_dependencies.sh
RUN echo "source /vt/local/env.sh" >> /etc/bash.bashrc

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Re-copy sources from working tree.
COPY --chown=vitess:vitess . /vt/src/vitess.io/vitess

# Build and install Vitess in a temporary output directory.
USER vitess

WORKDIR /vt/src/vitess.io/vitess
RUN make install PREFIX=/vt/install

ENV VTROOT /vt/src/vitess.io/vitess
ENV VTDATAROOT /vt/vtdataroot
ENV PATH $VTROOT/bin:$PATH
ENV PATH="/var/opt/etcd:${PATH}"

RUN mkdir /vt/local
COPY examples/local /vt/local

CMD cd /vt/local && ./101_initial_cluster.sh && /bin/bash
