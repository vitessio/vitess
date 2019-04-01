FROM vitess/bootstrap:common

# Install MariaDB 10.3
RUN apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com 0xF1656F24C74CD1D8 \
    && add-apt-repository 'deb [arch=amd64] http://ftp.osuosl.org/pub/mariadb/repo/10.3/debian stretch main' \
    && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       mariadb-server \
       libmariadbclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Bootstrap Vitess
WORKDIR /vt/src/vitess.io/vitess

ENV MYSQL_FLAVOR MariaDB103
USER vitess
RUN ./bootstrap.sh
