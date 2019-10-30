FROM vitess/bootstrap:percona80

# Re-copy sources from working tree
USER root
COPY . /vt/src/vitess.io/vitess

# Fix permissions
RUN chown -R vitess:vitess /vt
USER vitess

# Build Vitess
RUN make build
