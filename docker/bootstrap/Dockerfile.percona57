FROM vitess/bootstrap:common

# Install Percona 5.7
RUN apt-key adv --keyserver keys.gnupg.net \
        --recv-keys 8507EFA5 && \
    add-apt-repository 'deb http://repo.percona.com/apt stretch main' && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-5.7 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-5.7 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        percona-server-server-5.7 libperconaserverclient18.1-dev && \
    rm -rf /var/lib/apt/lists/*

# Bootstrap Vitess
WORKDIR /vt/src/github.com/youtube/vitess
USER vitess
# Required by e2e test dependencies e.g. test/environment.py.
ENV USER vitess
ENV MYSQL_FLAVOR MySQL56
RUN ./bootstrap.sh --skip_root_installs
