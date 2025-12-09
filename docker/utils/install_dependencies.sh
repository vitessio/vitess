#!/bin/bash

# This is a script that gets run as part of the Dockerfile build
# to install dependencies for the vitess/lite family of images.
#
# Usage: install_dependencies.sh <flavor> <version (optional)>

set -euo pipefail

FLAVOR="$1"
VERSION=""
if [ $# -eq 2 ]; then
  VERSION="$2"
fi

export DEBIAN_FRONTEND=noninteractive

KEYSERVERS=(
    keyserver.ubuntu.com
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
    libwww-perl
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
    zstd
)

apt-get update
apt-get install -y --no-install-recommends "${BASE_PACKAGES[@]}"

# Packages specific to certain flavors.
case "${FLAVOR}" in
mysql80)
    if [ -z "$VERSION" ]; then
        VERSION=8.0.43
    fi
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-common_${VERSION}-1debian12_amd64.deb /tmp/mysql-common_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/libmysqlclient21_${VERSION}-1debian12_amd64.deb /tmp/libmysqlclient21_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-core_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-client_${VERSION}-1debian12_amd64.deb /tmp/mysql-client_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server-core_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-server_${VERSION}-1debian12_amd64.deb /tmp/mysql-server_${VERSION}-1debian12_amd64.deb
    PACKAGES=(
        /tmp/mysql-common_${VERSION}-1debian12_amd64.deb
        /tmp/libmysqlclient21_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-client_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-server_${VERSION}-1debian12_amd64.deb
        mysql-shell
        percona-xtrabackup-80
    )
    ;;
mysql84)
    if [ -z "$VERSION" ]; then
        VERSION=8.4.6
    fi
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-common_${VERSION}-1debian12_amd64.deb /tmp/mysql-common_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/libmysqlclient24_${VERSION}-1debian12_amd64.deb /tmp/libmysqlclient24_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-core_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-client_${VERSION}-1debian12_amd64.deb /tmp/mysql-client_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server-core_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server_${VERSION}-1debian12_amd64.deb /tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb
    do_fetch https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-server_${VERSION}-1debian12_amd64.deb /tmp/mysql-server_${VERSION}-1debian12_amd64.deb
    PACKAGES=(
        /tmp/mysql-common_${VERSION}-1debian12_amd64.deb
        /tmp/libmysqlclient24_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-client_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb
        /tmp/mysql-server_${VERSION}-1debian12_amd64.deb
        mysql-shell
        percona-xtrabackup-84
    )
    ;;
percona80)
    PACKAGES=(
        libperconaserverclient21
        percona-server-rocksdb
        percona-server-server
        percona-xtrabackup-80
    )
    ;;
percona84)
    PACKAGES=(
        libperconaserverclient22
        percona-telemetry-agent
        percona-server-rocksdb
        percona-server-server
        percona-xtrabackup-84
    )
    ;;
*)
    echo "Unknown flavor ${FLAVOR}"
    exit 1
    ;;
esac

# Get GPG keys for extra apt repositories.
# repo.mysql.com
add_apt_key 8C718D3B5072E1F5
add_apt_key A8D3785C

# All flavors include Percona XtraBackup (from repo.percona.com).
add_apt_key 9334A25F8507EFA5

# Add extra apt repositories for MySQL.
case "${FLAVOR}" in
mysql80)
    echo 'deb http://repo.mysql.com/apt/debian/ bookworm mysql-8.0' > /etc/apt/sources.list.d/mysql.list
    ;;
mysql84)
    echo 'deb http://repo.mysql.com/apt/debian/ bookworm mysql-8.4-lts' > /etc/apt/sources.list.d/mysql.list
    ;;
esac

# Add extra apt repositories for Percona Server and/or Percona XtraBackup.
case "${FLAVOR}" in
mysql80)
    echo 'deb http://repo.percona.com/apt bookworm main' > /etc/apt/sources.list.d/percona.list
    ;;
mysql84)
    echo 'deb http://repo.percona.com/pxb-84-lts/apt bookworm main' > /etc/apt/sources.list.d/percona.list
    ;;
percona80)
    echo 'deb http://repo.percona.com/apt bookworm main' > /etc/apt/sources.list.d/percona.list
    echo 'deb http://repo.percona.com/ps-80/apt bookworm main' > /etc/apt/sources.list.d/percona80.list
    ;;
percona84)
    echo 'deb http://repo.percona.com/apt bookworm main' > /etc/apt/sources.list.d/percona.list
    echo 'deb http://repo.percona.com/pxb-84-lts/apt bookworm main' >> /etc/apt/sources.list.d/percona.list
    echo 'deb http://repo.percona.com/telemetry/apt bookworm main' > /etc/apt/sources.list.d/percona-telemetry.list
    echo 'deb http://repo.percona.com/ps-84-lts/apt bookworm main' > /etc/apt/sources.list.d/percona84.list
    ;;
esac

# Pre-fill values for installation prompts that are normally interactive.
case "${FLAVOR}" in
percona80)
    debconf-set-selections <<EOF
debconf debconf/frontend select Noninteractive
percona-server-server-8.0 percona-server-server/root_password password 'unused'
percona-server-server-8.0 percona-server-server/root_password_again password 'unused'
EOF
    ;;
percona84)
    debconf-set-selections <<EOF
debconf debconf/frontend select Noninteractive
percona-server-server-8.4 percona-server-server/root_password password 'unused'
percona-server-server-8.4 percona-server-server/root_password_again password 'unused'
EOF
    ;;
esac

# Install flavor-specific packages
apt-get update
for i in $(seq 1 $MAX_RETRY); do apt-get install -y --no-install-recommends "${PACKAGES[@]}" && break; done
if [[ "$i" = "$MAX_RETRY" ]]; then
    exit 1
fi

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*
rm -rf /var/lib/mysql/
rm -rf /tmp/*.deb
rm -rf /etc/apt/sources.list.d/mysql.list /etc/apt/sources.list.d/percona*.list
