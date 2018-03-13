# We rely on the base image, as that will re-copy the local
# working tree and build it. Because of so, we trust the local
# vendor folder is up to data.
FROM vitess/base

# Clean local files, and keep vendorer libs
RUN git clean -xdf --exclude="vendor"

RUN mkdir /vt/releases

CMD tar -czf /vt/releases/v$VERSION.tar.gz --exclude .git .