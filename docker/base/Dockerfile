# NOTE: This file is also symlinked as "Dockerfile" in the root of our
#       repository because the automated build feature on Docker Hub does not
#       allow to specify a different build context. It always assumes that the
#       build context is the same directory as the Dockerfile is in.
#       "make build" below must be called in our repository's root and
#       therefore we need to have the symlinked "Dockerfile" in there as well.
# TODO(mberlin): Remove the symlink and this note once
# https://github.com/docker/hub-feedback/issues/292 is fixed.
FROM vitess/bootstrap:mysql57

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Re-copy sources from working tree
USER root
COPY . /vt/src/vitess.io/vitess

# Build Vitess
RUN make build

# Fix permissions
RUN chown -R vitess:vitess /vt
USER vitess

