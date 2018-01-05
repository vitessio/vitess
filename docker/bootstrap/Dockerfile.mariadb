FROM vitess/bootstrap:common

# Install MariaDB 10.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    mariadb-server \
    libmariadbclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Bootstrap Vitess
WORKDIR /vt/src/github.com/youtube/vitess
USER vitess
# Required by e2e test dependencies e.g. test/environment.py.
ENV USER vitess
ENV MYSQL_FLAVOR MariaDB
RUN ./bootstrap.sh --skip_root_installs
