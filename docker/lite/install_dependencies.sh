#!/bin/bash

# This is a script that gets run as part of the Dockerfile build
# to install dependencies for the vitess/lite family of images.
#
# Usage: install_dependencies.sh <flavor>

set -euo pipefail

FLAVOR="$1"
export DEBIAN_FRONTEND=noninteractive

retry() {
    for i in $(seq 1 10); do
        if "$@"; then return; fi
    done
}

# Install base packages that are common to all flavors.
BASE_PACKAGES=(
    bzip2
    ca-certificates
    dirmngr
    gnupg
    libaio1
    libatomic1
    libcurl3
    libdbd-mysql-perl
    libev4
    libjemalloc1
    libtcmalloc-minimal4
    procps
    rsync
    strace
    sysstat
    wget
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
        percona-xtrabackup-24
    )
    ;;
mysql57)
    PACKAGES=(
        libmysqlclient20
        mysql-client
        mysql-server
        percona-xtrabackup-24
    )
    ;;
mysql80)
    PACKAGES=(
        libmysqlclient21
        mysql-client
        mysql-server
        percona-xtrabackup-80
    )
    ;;
percona)
    PACKAGES=(
        percona-server-server-5.6
        percona-xtrabackup-24
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
mariadb|mariadb103)
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
    retry apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys 8C718D3B5072E1F5
    ;;
mariadb|mariadb103)
    # digitalocean.com
    retry apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys F1656F24C74CD1D8
    ;;
esac

# All flavors (except mariadb*) include Percona XtraBackup (from repo.percona.com).
retry apt-key adv --no-tty --keyserver keys.gnupg.net --recv-keys 9334A25F8507EFA5

# Add extra apt repositories for MySQL.
case "${FLAVOR}" in
mysql56)
    echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-5.6' > /etc/apt/sources.list.d/mysql.list
    ;;
mysql57)
    echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7' > /etc/apt/sources.list.d/mysql.list
    ;;
mysql80)
    echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-8.0' > /etc/apt/sources.list.d/mysql.list
    ;;
mariadb)
    echo 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.2/debian stretch main' > /etc/apt/sources.list.d/mariadb.list
    ;;
mariadb103)
    echo 'deb http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.3/debian stretch main' > /etc/apt/sources.list.d/mariadb.list
    ;;
esac

# Add extra apt repositories for Percona Server and/or Percona XtraBackup.
case "${FLAVOR}" in
mysql56|mysql57|mysql80|percona|percona57)
    echo 'deb http://repo.percona.com/apt stretch main' > /etc/apt/sources.list.d/percona.list
    ;;
percona80)
    echo 'deb http://repo.percona.com/apt stretch main' > /etc/apt/sources.list.d/percona.list
    echo 'deb http://repo.percona.com/ps-80/apt stretch main' > /etc/apt/sources.list.d/percona80.list
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
apt-get install -y --no-install-recommends "${PACKAGES[@]}"
 
 # Clean up files we won't need in the final image.
 rm -rf /var/lib/apt/lists/*
 rm -rf /var/lib/mysql/
