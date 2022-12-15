# DO NOT MODIFY: THIS FILE IS GENERATED USING "make generate_ci_workflows"

ARG bootstrap_version=14
ARG image="vitess/bootstrap:${bootstrap_version}-mysql80"

FROM "${image}"

USER root

# Re-copy sources from working tree
RUN rm -rf /vt/src/vitess.io/vitess/*
COPY . /vt/src/vitess.io/vitess

# Set the working directory
WORKDIR /vt/src/vitess.io/vitess

# Fix permissions
RUN chown -R vitess:vitess /vt

USER vitess

# Set environment variables
ENV VTROOT /vt/src/vitess.io/vitess
# Set the vtdataroot such that it uses the volume mount
ENV VTDATAROOT /vt/vtdataroot

# create the vtdataroot directory
RUN mkdir -p $VTDATAROOT

# install goimports
RUN go install golang.org/x/tools/cmd/goimports@latest

# sleep for 50 minutes
CMD sleep 3000
