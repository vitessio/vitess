ARG bootstrap_version=8
ARG image="vitess/bootstrap:${bootstrap_version}-common"

FROM "${image}"

RUN apt-get update
RUN apt-get install -y sudo curl vim jq git

# Install dependencies
COPY docker/lite/install_dependencies.sh /vt/dist/install_dependencies.sh
RUN /vt/dist/install_dependencies.sh mysql57

COPY docker/local/install_local_dependencies.sh /vt/dist/install_local_dependencies.sh
RUN /vt/dist/install_local_dependencies.sh

RUN echo "source /vt/local/env.sh" >> /etc/bash.bashrc
RUN chsh -s /bin/bash vitess

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

RUN mkdir /vt/common
COPY examples/common /vt/common

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
RUN ["bash", "-c", "echo 'source $HOME/.nvm/nvm.sh' >> $HOME/.bashrc"]
RUN ["bash", "-c", "source $HOME/.nvm/nvm.sh; nvm install v16.13.0"]

CMD cd /vt/local && ./101_initial_cluster.sh && /bin/bash
