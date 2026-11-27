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
<<<<<<< HEAD
    wget \
        --tries=$MAX_RETRY\
        --read-timeout=30\
        --timeout=30\
        --retry-connrefused\
        --waitretry=1\
        --no-dns-cache \
        "$1" -O "$2"
||||||| parent of 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
	wget \
		--tries=$MAX_RETRY --read-timeout=30 --timeout=30 --retry-connrefused --waitretry=1 --no-dns-cache \
		"$1" -O "$2"
}

install_mysql_tarball() {
	local series="$1"
	local version="$2"
	local archive="mysql-${version}-linux-glibc2.28-aarch64.tar.xz"
	local source="/tmp/${archive}"

	do_fetch "https://cdn.mysql.com/Downloads/MySQL-${series}/${archive}" "${source}"
	tar -xJf "${source}" -C /usr/local --strip-components=1
}

percona_mysql_shell_package() {
	local version="$1"
	printf 'percona-mysql-shell=%s-1-1.%s\n' "${version}" "${DEBIAN_CODENAME}"
}

# Oracle's Debian repo does not publish arm64 .deb packages for the MySQL
# flavors we use here, so on arm64 we install `mysql80` and `mysql84` from
# Oracle's generic aarch64 tarballs and pair them with Percona's arm64
# `percona-mysql-shell` and xtrabackup packages.
#
# Those Oracle tarballs still link against libaio.so.1, but trixie's t64
# transition renamed the packaged soname to libaio.so.1t64. Create a
# compatibility symlink so the tarball-installed mysqld starts successfully on
# arm64 trixie images.
ensure_mysql_tarball_compat() {
	local libdir="/usr/lib/aarch64-linux-gnu"

	if [[ "${ARCH}" != "arm64" ]]; then
		return
	fi

	if [[ "${FLAVOR}" != "mysql80" && "${FLAVOR}" != "mysql84" ]]; then
		return
	fi

	if [[ "${LIBAIO}" != "libaio1t64" ]]; then
		return
	fi

	if [[ ! -e "${libdir}/libaio.so.1" ]]; then
		ln -s "${libdir}/libaio.so.1t64" "${libdir}/libaio.so.1"
	fi
=======
	wget \
		--tries=$MAX_RETRY --read-timeout=30 --timeout=30 --retry-connrefused --waitretry=1 --no-dns-cache \
		"$1" -O "$2"
}

install_mysql_tarball() {
	local series="$1"
	local version="$2"
	local archive="mysql-${version}-linux-glibc2.28-aarch64.tar.xz"
	local source="/tmp/${archive}"

	do_fetch "https://cdn.mysql.com/Downloads/MySQL-${series}/${archive}" "${source}"
	tar -xJf "${source}" -C /usr/local --strip-components=1
}

install_mysql_shell_tarball() {
	local version="$1"
	local archive="mysql-shell-${version}-linux-glibc2.28-arm-64bit.tar.gz"
	local source="/tmp/${archive}"

	do_fetch "https://cdn.mysql.com/Downloads/MySQL-Shell/${archive}" "${source}"
	tar -xzf "${source}" -C /usr/local --strip-components=1
}

percona_mysql_shell_package() {
	local version="$1"
	printf 'percona-mysql-shell=%s-1-1.%s\n' "${version}" "${DEBIAN_CODENAME}"
}

# Oracle's Debian repo does not publish arm64 .deb packages for the MySQL
# flavors we use here, so on arm64 we install `mysql80` and `mysql84` from
# Oracle's generic aarch64 tarballs and pair them with Percona's arm64
# xtrabackup packages. MySQL Shell comes from Percona's arm64
# `percona-mysql-shell` package for `mysql80`, and from Oracle's generic arm64
# MySQL Shell tarball for `mysql84`.
#
# Those Oracle tarballs still link against libaio.so.1, but trixie's t64
# transition renamed the packaged soname to libaio.so.1t64. Create a
# compatibility symlink so the tarball-installed mysqld starts successfully on
# arm64 trixie images.
ensure_mysql_tarball_compat() {
	local libdir="/usr/lib/aarch64-linux-gnu"

	if [[ "${ARCH}" != "arm64" ]]; then
		return
	fi

	if [[ "${FLAVOR}" != "mysql80" && "${FLAVOR}" != "mysql84" ]]; then
		return
	fi

	if [[ "${LIBAIO}" != "libaio1t64" ]]; then
		return
	fi

	if [[ ! -e "${libdir}/libaio.so.1" ]]; then
		ln -s "${libdir}/libaio.so.1t64" "${libdir}/libaio.so.1"
	fi
>>>>>>> 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
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
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-common_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-common_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/libmysqlclient21_${VERSION}-1debian12_amd64.deb" "/tmp/libmysqlclient21_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-core_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-client_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-client_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-client_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server-core_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-community-server_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-community/mysql-server_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-server_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.0/m/mysql-shell/mysql-shell_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-shell_${VERSION}-1debian12_amd64.deb"
    PACKAGES=(
        "/tmp/mysql-common_${VERSION}-1debian12_amd64.deb"
        "/tmp/libmysqlclient21_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-client_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-server_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-shell_${VERSION}-1debian12_amd64.deb"
        percona-xtrabackup-80
    )
    ;;
mysql84)
<<<<<<< HEAD
    if [ -z "$VERSION" ]; then
        VERSION=8.4.6
    fi
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-common_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-common_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/libmysqlclient24_${VERSION}-1debian12_amd64.deb" "/tmp/libmysqlclient24_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-core_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-client_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-client_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server-core_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-server_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-server_${VERSION}-1debian12_amd64.deb"
    do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-shell/mysql-shell_${VERSION}-1debian12_amd64.deb" "/tmp/mysql-shell_${VERSION}-1debian12_amd64.deb"
    PACKAGES=(
        "/tmp/mysql-common_${VERSION}-1debian12_amd64.deb"
        "/tmp/libmysqlclient24_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client-core_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client-plugins_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-client_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-client_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-server-core_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-community-server_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-server_${VERSION}-1debian12_amd64.deb"
        "/tmp/mysql-shell_${VERSION}-1debian12_amd64.deb"
        percona-xtrabackup-84
    )
    ;;
||||||| parent of 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
	if [ -z "$VERSION" ]; then
		VERSION=8.4.8
	fi
	if [[ "${ARCH}" == "arm64" ]]; then
		install_mysql_tarball "8.4" "${VERSION}"
		PACKAGES=(
			"$(percona_mysql_shell_package "${VERSION}")"
			percona-xtrabackup-84
		)
	else
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
	fi
	;;
=======
	if [ -z "$VERSION" ]; then
		VERSION=8.4.13.1
	fi
	# MySQL Shell has its own version: as of 26.7, it has one release series
	# for all supported MySQL Server versions, and its 8.4 series is
	# discontinued.
	MYSQL_SHELL_VERSION=26.10.0.1
	if [[ "${ARCH}" == "arm64" ]]; then
		install_mysql_tarball "8.4" "${VERSION}"
		install_mysql_shell_tarball "${MYSQL_SHELL_VERSION}"
		PACKAGES=(
			percona-xtrabackup-84
		)
	else
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-common_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/libmysqlclient24_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/libmysqlclient24_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client-plugins_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-client_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server-core_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-community-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-8.4-lts/m/mysql-community/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-server_${VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
		do_fetch "https://repo.mysql.com/apt/debian/pool/mysql-innovation/m/mysql-shell/mysql-shell_${MYSQL_SHELL_VERSION}-1debian${DEBIAN_VERSION}_amd64.deb" "/tmp/mysql-shell_${MYSQL_SHELL_VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
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
			"/tmp/mysql-shell_${MYSQL_SHELL_VERSION}-1debian${DEBIAN_VERSION}_amd64.deb"
			percona-xtrabackup-84
		)
	fi
	;;
>>>>>>> 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
percona80)
    PACKAGES=(
        libperconaserverclient21
        percona-server-rocksdb
        percona-server-server
        percona-xtrabackup-80
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
<<<<<<< HEAD
    echo 'deb http://repo.mysql.com/apt/debian/ bookworm mysql-8.4-lts' > /etc/apt/sources.list.d/mysql.list
    ;;
||||||| parent of 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
	if [[ "${ARCH}" == "arm64" ]]; then
		echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/pxb-84-lts/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
		echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/ps-84-lts/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona-shell.list
	else
		echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ ${DEBIAN_CODENAME} mysql-8.4-lts" > /etc/apt/sources.list.d/mysql.list
	fi
	;;
=======
	if [[ "${ARCH}" == "arm64" ]]; then
		echo "deb [signed-by=/etc/apt/keyrings/percona.gpg] http://repo.percona.com/pxb-84-lts/apt ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/percona.list
	else
		echo "deb [signed-by=/etc/apt/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian/ ${DEBIAN_CODENAME} mysql-8.4-lts" > /etc/apt/sources.list.d/mysql.list
	fi
	;;
>>>>>>> 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
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
<<<<<<< HEAD
||||||| parent of 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
rm -rf /tmp/*.tar.xz
rm -rf /etc/apt/sources.list.d/mysql.list /etc/apt/sources.list.d/percona*.list /etc/apt/keyrings/mysql.gpg /etc/apt/keyrings/percona.gpg
=======
rm -rf /tmp/*.tar.xz
rm -rf /tmp/*.tar.gz
rm -rf /etc/apt/sources.list.d/mysql.list /etc/apt/sources.list.d/percona*.list /etc/apt/keyrings/mysql.gpg /etc/apt/keyrings/percona.gpg
>>>>>>> 5bf7d70203 (docker: install MySQL 8.4.13.1 in the MySQL 8.4 images (#22154))
