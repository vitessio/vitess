FROM debian:jessie

# Install Percona XtraDB Cluster (Galera)
RUN apt-key adv --keyserver keys.gnupg.net --recv-keys 8507EFA5 && \
    echo 'deb http://repo.percona.com/apt jessie main' > /etc/apt/sources.list.d/mysql.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        percona-xtradb-cluster-server-5.6 && \
    rm -rf /var/lib/apt/lists/*

# Set up Orchestrator database
RUN service mysql start && \
    mysql -e "CREATE DATABASE orchestrator; GRANT ALL PRIVILEGES ON orchestrator.* TO 'orc_server_user'@'127.0.0.1' IDENTIFIED BY 'orc_server_user_password'" && \
    service mysql stop

# Copy Orchestrator files (placed in workdir by build.sh)
COPY vtctlclient /usr/bin/vtctlclient
COPY orchestrator /usr/bin/orchestrator
COPY orchestrator.conf.json /orc/conf/orchestrator.conf.json
COPY resources /orc/resources

WORKDIR /orc
CMD ["/usr/bin/orchestrator", "http"]

