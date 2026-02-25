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

# Detect Debian version from the runtime OS. Used for:
#  - APT source lines (codename: bookworm, trixie, ...)
#  - MySQL .deb package URLs (debian12, debian13, ...)
#  - Library package renames (time_t ABI transition in trixie)
DEBIAN_CODENAME=$(sed -n 's/^VERSION_CODENAME=//p' /etc/os-release)
DEBIAN_VERSION=$(sed -n 's/^VERSION_ID="\?\([0-9]*\).*/\1/p' /etc/os-release)
if [[ "${DEBIAN_CODENAME}" == "trixie" ]]; then
	LIBAIO=libaio1t64
	LIBCURL=libcurl4t64
	LIBEV=libev4t64
	LIBTCMALLOC=libtcmalloc-minimal4t64
else
	LIBAIO=libaio1
	LIBCURL=libcurl4
	LIBEV=libev4
	LIBTCMALLOC=libtcmalloc-minimal4
fi

# Set number of times to retry a download
MAX_RETRY=20

MYSQL_GPG_KEY="B7B3B788A8D3785C"

do_fetch() {
	wget \
		--tries=$MAX_RETRY --read-timeout=30 --timeout=30 --retry-connrefused --waitretry=1 --no-dns-cache \
		"$1" -O "$2"
}

# Install base packages that are common to all flavors.
BASE_PACKAGES=(
	bzip2
	ca-certificates
	dirmngr
	gnupg
	"$LIBAIO"
	libatomic1
	"$LIBCURL"
	libdbd-mysql-perl
	libwww-perl
	"$LIBEV"
	libjemalloc2
	"$LIBTCMALLOC"
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
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/libmysqlclient21_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/libmysqlclient21_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-shell/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	PACKAGES=(
		"/tmp/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/libmysqlclient21_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		percona-xtrabackup-80
	)
	;;
mysql84)
	if [ -z "$VERSION" ]; then
		VERSION=8.4.8
	fi
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/libmysqlclient24_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/libmysqlclient24_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-shell/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
	PACKAGES=(
		"/tmp/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/libmysqlclient24_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		"/tmp/mysql-shell_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
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
mkdir -p /etc/apt/keyrings

# repo.mysql.com - fetch the current MySQL GPG key from a public keyserver.
do_fetch "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x${MYSQL_GPG_KEY}&options=mr" /tmp/mysql-key.asc
gpg --batch --dearmor -o /etc/apt/keyrings/mysql.gpg < /tmp/mysql-key.asc
rm -f /tmp/mysql-key.asc

# repo.percona.com - extract keyring from official percona-release package.
do_fetch https://repo.percona.com/apt/percona-release_latest.generic_all.deb /tmp/percona-release.deb
dpkg-deb -x /tmp/percona-release.deb /tmp/percona-release-extract
cp /tmp/percona-release-extract/usr/share/keyrings/percona-keyring.gpg /etc/apt/keyrings/percona.gpg
rm -rf /tmp/percona-release.deb /tmp/percona-release-extract

# Add extra apt repositories for MySQL.
case "${FLAVOR}" in
mysql80)
    echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ ${DEBIAN_CODENAME} mysql-8.0" > /etc/apt/sources.list.d/mysql.list
    ;;
mysql84)
    echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ ${DEBIAN_CODENAME} mysql-8.4-lts" > /etc/apt/sources.list.d/mysql.list
    ;;
esac

# Add extra apt repositories for Percona Server and/or Percona XtraBackup.
case "${FLAVOR}" in
mysql80)
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
    ;;
mysql84)
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/pxb-84-lts/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
    ;;
percona80)
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/ps-80/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona80.list
    ;;
percona84)
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/pxb-84-lts/apt ${DEBIAN_CODENAME} main" >> /etc/apt/sources.list.d/percona.list
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/telemetry/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona-telemetry.list
    echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/ps-84-lts/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona84.list
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
rm -rf /etc/apt/sources.list.d/mysql.list /etc/apt/sources.list.d/percona*.list /etc/apt/keyrings/mysql.gpg /etc/apt/keyrings/percona.gpg
