FROM vitess/bootstrap:mariadb

# Re-copy sources from working tree
COPY . /vt/src/github.com/youtube/vitess

# Fix permissions
USER root
RUN chown -R vitess:vitess /vt
USER vitess

# Build Vitess
RUN make build
