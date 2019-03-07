FROM vitess/bootstrap:mariadb103

# Re-copy sources from working tree
USER root
COPY . /vt/src/vitess.io/vitess

# Build Vitess
RUN make build

# Fix permissions
RUN chown -R vitess:vitess /vt
USER vitess

