FROM vitess/bootstrap:common

# Install Percona 5.6
RUN for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keys.gnupg.net --recv-keys 9334A25F8507EFA5 && break; done && \
    add-apt-repository 'deb http://repo.percona.com/apt stretch main' && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-5.6 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-5.6 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        percona-server-server-5.6 libperconaserverclient18.1-dev rsync libev4 && \
    rm -rf /var/lib/apt/lists/* && \
    wget https://www.percona.com/downloads/XtraBackup/Percona-XtraBackup-2.4.13/binary/debian/stretch/x86_64/percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb && \
    dpkg -i percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb && \
    rm -f percona-xtrabackup-24_2.4.13-1.stretch_amd64.deb

# Bootstrap Vitess
WORKDIR /vt/src/vitess.io/vitess

ENV MYSQL_FLAVOR MySQL56
USER vitess
RUN ./bootstrap.sh
