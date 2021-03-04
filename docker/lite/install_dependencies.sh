#!/bin/bash

# This is a script that gets run as part of the Dockerfile build
# to install dependencies for the vitess/lite family of images.
#
# Usage: install_dependencies.sh <flavor>

set -euo pipefail

FLAVOR="$1"
export DEBIAN_FRONTEND=noninteractive

KEYSERVERS=(
    ha.pool.sks-keyservers.net
    keyserver.ubuntu.com
    hkp://p80.pool.sks-keyservers.net:80
    hkp://keyserver.ubuntu.com:80
)

add_apt_key() {
    for i in {1..3}; do
        for keyserver in "${KEYSERVERS[@]}"; do
            if apt-key adv --no-tty --keyserver "${keyserver}" --recv-keys "$1"; then return; fi
        done
    done
}

# Set number of times to retry a download
MAX_RETRY=20

do_fetch() {
    wget \
        --tries=$MAX_RETRY\
        --read-timeout=30\
        --timeout=30\
        --retry-connrefused\
        --waitretry=1\
        --no-dns-cache \
        $1 -O $2
}

# Install base packages that are common to all flavors.
BASE_PACKAGES=(
    bzip2
    ca-certificates
    dirmngr
    gnupg
    libaio1
    libatomic1
    libcurl4
    libdbd-mysql-perl
    libev4
    libjemalloc2
    libtcmalloc-minimal4
    procps
    rsync
    strace
    sysstat
    wget
    curl
    percona-toolkit
)

apt-get update
apt-get install -y --no-install-recommends "${BASE_PACKAGES[@]}"

# Packages specific to certain flavors.
case "${FLAVOR}" in
mysql56)
    PACKAGES=(
        libmysqlclient18
        mysql-client
        mysql-server
        libcurl3
        percona-xtrabackup
    )
    ;;
mysql57)
    mysql57_version=5.7.31
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/libmysqlclient20_${mysql57_version}-1debian10_amd64.deb /tmp/libmysqlclient20_${mysql57_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/mysql-community-client_${mysql57_version}-1debian10_amd64.deb /tmp/mysql-community-client_${mysql57_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/mysql-client_${mysql57_version}-1debian10_amd64.deb /tmp/mysql-client_${mysql57_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/mysql-community-server_${mysql57_version}-1debian10_amd64.deb /tmp/mysql-community-server_${mysql57_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/mysql-server_${mysql57_version}-1debian10_amd64.deb /tmp/mysql-server_${mysql57_version}-1debian10_amd64.deb
    PACKAGES=(
        /tmp/libmysqlclient20_${mysql57_version}-1debian10_amd64.deb
        /tmp/mysql-community-client_${mysql57_version}-1debian10_amd64.deb
        /tmp/mysql-client_${mysql57_version}-1debian10_amd64.deb
        /tmp/mysql-community-server_${mysql57_version}-1debian10_amd64.deb
        /tmp/mysql-server_${mysql57_version}-1debian10_amd64.deb
        percona-xtrabackup-24
    )
    ;;
mysql80)
    mysql8_version=8.0.21
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/libmysqlclient21_${mysql8_version}-1debian10_amd64.deb /tmp/libmysqlclient21_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-core_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-community-client-core_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-community-client_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-client_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-client_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server-core_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-community-server-core_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-community-server_${mysql8_version}-1debian10_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-server_${mysql8_version}-1debian10_amd64.deb /tmp/mysql-server_${mysql8_version}-1debian10_amd64.deb
    PACKAGES=(
        /tmp/libmysqlclient21_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-community-client-core_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-community-client_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-client_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-community-server-core_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-community-server_${mysql8_version}-1debian10_amd64.deb
        /tmp/mysql-server_${mysql8_version}-1debian10_amd64.deb
        percona-xtrabackup-80
    )
    ;;
percona)
    PACKAGES=(
        libcurl3
        percona-server-server-5.6
        percona-xtrabackup
    )
    ;;
percona57)
    PACKAGES=(
        libperconaserverclient20
        percona-server-server-5.7
        percona-xtrabackup-24
    )
    ;;
percona80)
    PACKAGES=(
        libperconaserverclient21
        percona-server-rocksdb
        percona-server-server
        percona-server-tokudb
        percona-xtrabackup-80
    )
    ;;
mariadb)
    PACKAGES=(
        mariadb-server-10.2
    )
    ;;
mariadb103)
    PACKAGES=(
        mariadb-server
    )
    ;;
*)
    echo "Unknown flavor ${FLAVOR}"
    exit 1
    ;;
esac

# Get GPG keys for extra apt repositories.
case "${FLAVOR}" in
mysql56|mysql57|mysql80)
    # repo.mysql.com
    add_apt_key 8C718D3B5072E1F5
    ;;
mariadb|mariadb103)
    # digitalocean.com
    add_apt_key F1656F24C74CD1D8
    ;;
esac

# All flavors (except mariadb*) include Percona XtraBackup (from repo.percona.com).
add_apt_key 9334A25F8507EFA5

# Add extra apt repositories for MySQL.
case "${FLAVOR}" in
mysql56)
    echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-5.6' > /etc/apt/sources.list.d/mysql.list
    ;;
mysql57)
    echo 'deb http://repo.mysql.com/apt/debian/ buster mysql-5.7' > /etc/apt/sources.list.d/mysql.list
    ;;
mysql80)
    echo 'deb http://repo.mysql.com/apt/debian/ buster mysql-8.0' > /etc/apt/sources.list.d/mysql.list
    ;;
mariadb)
    echo 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.2/debian stretch main' > /etc/apt/sources.list.d/mariadb.list
    ;;
mariadb103)
    echo 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.3/debian buster main' > /etc/apt/sources.list.d/mariadb.list
    ;;
esac

# Add extra apt repositories for Percona Server and/or Percona XtraBackup.
case "${FLAVOR}" in
percona|mysql56)
    # 5.6 is no longer supported on buster
    echo 'deb http://deb.debian.org/debian stretch main' > /etc/apt/sources.list.d/stretch.list
    echo 'deb http://repo.percona.com/apt stretch main' > /etc/apt/sources.list.d/percona.list
    ;;
mysql57|mysql80|percona57)
    echo 'deb http://repo.percona.com/apt buster main' > /etc/apt/sources.list.d/percona.list
    ;;
percona80)
    echo 'deb http://repo.percona.com/apt buster main' > /etc/apt/sources.list.d/percona.list
    echo 'deb http://repo.percona.com/ps-80/apt buster main' > /etc/apt/sources.list.d/percona80.list
    ;;
esac

# Pre-fill values for installation prompts that are normally interactive.
case "${FLAVOR}" in
percona)
    debconf-set-selections <<EOF
debconf debconf/frontend select Noninteractive
percona-server-server-5.6 percona-server-server/root_password password 'unused'
percona-server-server-5.6 percona-server-server/root_password_again password 'unused'
EOF
    ;;
percona57)
    debconf-set-selections <<EOF
debconf debconf/frontend select Noninteractive
percona-server-server-5.7 percona-server-server/root_password password 'unused'
percona-server-server-5.7 percona-server-server/root_password_again password 'unused'
percona-server-server-5.7 percona-server-server-5.7/root-pass password 'unused'
percona-server-server-5.7 percona-server-server-5.7/re-root-pass password 'unused'
EOF
    ;;
percona80)
    debconf-set-selections <<EOF
debconf debconf/frontend select Noninteractive
percona-server-server-8.0 percona-server-server/root_password password 'unused'
percona-server-server-8.0 percona-server-server/root_password_again password 'unused'
EOF
    ;;
esac

# Install flavor-specific packages
apt-get update
for i in $(seq 1 $MAX_RETRY); do apt-get install -y --no-install-recommends "${PACKAGES[@]}" && break; done

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*
rm -rf /var/lib/mysql/
rm -rf /tmp/*.deb
