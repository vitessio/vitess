FROM vitess/bootstrap:common

# Install Percona 8.0
RUN for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keys.gnupg.net --recv-keys 9334A25F8507EFA5 && break; done \
    && echo 'deb http://repo.percona.com/ps-80/apt stretch main' > /etc/apt/sources.list.d/percona.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-8.0 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-8.0 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        percona-server-server \
	libperconaserverclient21 \
	percona-server-tokudb \
	percona-server-rocksdb \
	bzip2 \
	libdbd-mysql-perl \
	rsync \
	libev4 \
    && rm -rf /var/lib/apt/lists/* \
    && wget https://www.percona.com/downloads/XtraBackup/Percona-XtraBackup-8.0.4/binary/debian/stretch/x86_64/percona-xtrabackup-80_8.0.4-1.stretch_amd64.deb \
    && dpkg -i percona-xtrabackup-80_8.0.4-1.stretch_amd64.deb \
    && rm -f percona-xtrabackup-80_8.0.4-1.stretch_amd64.deb

# Bootstrap Vitess
WORKDIR /vt/src/vitess.io/vitess

ENV MYSQL_FLAVOR MySQL80
USER vitess
RUN ./bootstrap.sh
