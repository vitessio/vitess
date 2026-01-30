#!/bin/bash
# shellcheck disable=SC2164

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### This file is executed by 'make tools'. You do not need to execute it directly.

source ./dev.env

# Outline of this file.
# 0. Initialization and helper methods.
# 1. Installation of dependencies.

BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_CONSUL=${BUILD_CONSUL:-1}

VITESS_RESOURCES_DOWNLOAD_BASE_URL="https://github.com/vitessio/vitess-resources/releases/download"
VITESS_RESOURCES_RELEASE="v4.0"
VITESS_RESOURCES_DOWNLOAD_URL="${VITESS_RESOURCES_DOWNLOAD_BASE_URL}/${VITESS_RESOURCES_RELEASE}"
#
# 0. Initialization and helper methods.
#

[[ "$(dirname "$0")" = "." ]] || fail "bootstrap.sh must be run from its current directory"

# install_dep is a helper function to generalize the download and installation of dependencies.
#
# If the installation is successful, it puts the installed version string into
# the $dist/.installed_version file. If the version has not changed, bootstrap
# will skip future installations.
install_dep() {
	if [[ $# != 4 ]]; then
		fail "install_dep function requires exactly 4 parameters (and not $#). Parameters: $*"
	fi
	local name="$1"
	local version="$2"
	local dist="$3"
	local install_func="$4"

	version_file="$dist/.installed_version"
	if [[ -f "$version_file" && "$(cat "$version_file")" == "$version" ]]; then
		echo "skipping $name install. remove $dist to force re-install."
		return
	fi

	echo "<<< Installing $name $version >>>"

	# shellcheck disable=SC2064
	trap "fail '$name build failed'; exit 1" ERR

	# Cleanup any existing data and re-create the directory.
	rm -rf "$dist"
	mkdir -p "$dist"

	# Change $CWD to $dist before calling "install_func".
	pushd "$dist" >/dev/null
	# -E (same as "set -o errtrace") makes sure that "install_func" inherits the
	# trap. If here's an error, the trap will be called which will exit this
	# script.
	set -E
	$install_func "$version" "$dist"
	set +E
	popd >/dev/null

	trap - ERR

	echo "$version" >"$version_file"
}

#
# 1. Installation of dependencies.
#

# We should not use the arch command, since it is not reliably
# available on macOS or some linuxes:
# https://www.gnu.org/software/coreutils/manual/html_node/arch-invocation.html
get_arch() {
	uname -m
}

# verify_sha256 verifies the SHA256 checksum of a file.
#
# Usage: verify_sha256 <file> <expected_sha256>
verify_sha256() {
	local file="$1"
	local expected="$2"

	echo "Verifying SHA256 checksum for $file..."
	if command -v sha256sum &>/dev/null; then
		echo "$expected  $file" | sha256sum -c - || fail "SHA256 checksum verification failed for $file"
	elif command -v shasum &>/dev/null; then
		echo "$expected  $file" | shasum -a 256 -c - || fail "SHA256 checksum verification failed for $file"
	else
		fail "Neither sha256sum nor shasum found. Cannot verify checksum."
	fi
	echo "Checksum OK."
}

# verify_sha512 verifies the SHA512 checksum of a file.
#
# Usage: verify_sha512 <file> <expected_sha512>
verify_sha512() {
	local file="$1"
	local expected="$2"

	echo "Verifying SHA512 checksum for $file..."
	if command -v sha512sum &>/dev/null; then
		echo "$expected  $file" | sha512sum -c - || fail "SHA512 checksum verification failed for $file"
	elif command -v shasum &>/dev/null; then
		echo "$expected  $file" | shasum -a 512 -c - || fail "SHA512 checksum verification failed for $file"
	else
		fail "Neither sha512sum nor shasum found. Cannot verify checksum."
	fi
	echo "Checksum OK."
}

# Install protoc.
install_protoc() {
	local version="$1"
	local dist="$2"

	case $(uname) in
	Linux) local platform=linux ;;
	Darwin) local platform=osx ;;
	*)
		echo "ERROR: unsupported platform for protoc"
		exit 1
		;;
	esac

	case $(get_arch) in
	aarch64) local target=aarch_64 ;;
	x86_64) local target=x86_64 ;;
	arm64) case "$platform" in
		osx) local target=aarch_64 ;;
		*)
			echo "ERROR: unsupported architecture for protoc"
			exit 1
			;;
		esac ;;
	*)
		echo "ERROR: unsupported architecture for protoc"
		exit 1
		;;
	esac

	# SHA256 checksums for protoc v21.3 from official releases.
	local sha256
	case "${platform}-${target}" in
	linux-x86_64) sha256="214b670884972d09a1ccf7b7b4c65a12bdd73d55ebea6b5207b5eb2333ae32ec" ;;
	linux-aarch_64) sha256="e22ad6908e197ac326a02ddabc49046846b64d051f3bef16f5d3afd50ffdec18" ;;
	osx-x86_64) sha256="b988e06fed5279865445978efd97ec92990ca24b0fe16c04aafe85d6cee71eeb" ;;
	osx-aarch_64) sha256="3f1b2a59ba111e3227adf47e5513b3ea9133d9621dc5df70cd1ffed6dc756877" ;;
	*)
		echo "ERROR: no checksum for protoc $platform-$target"
		exit 1
		;;
	esac

	local file="protoc-${version}-${platform}-${target}.zip"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"

	ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
}

# Install Zookeeper.
install_zookeeper() {
	local version="$1"
	local dist="$2"
	local zk="zookeeper-$version"
	local file="apache-${zk}-bin.tar.gz"

	# SHA512 checksum for Zookeeper 3.9.4 from Apache archives.
	local sha512="36bffae6440ed0d71ed83a621b8c52c583860b414812197373237f0c148bd16e6b599977c90e5eb81c0fce6b82ef44aa782621535417cffc4c2a0a51a56f2cdf"

	"${VTROOT}/tools/wget-retry" -q "https://archive.apache.org/dist/zookeeper/${zk}/${file}"
	verify_sha512 "$dist/$file" "$sha512"
	tar -xzf "$dist/$file"
	mkdir -p "$dist"/lib
	cp "$dist/apache-$zk-bin/lib/"*.jar "$dist/lib/"
	rm -rf "$dist/apache-$zk-bin"
}

# Download and install etcd, link etcd binary into our root.
install_etcd() {
	local version="$1"
	local dist="$2"

	case $(uname) in
	Linux)
		local platform=linux
		local ext=tar.gz
		;;
	Darwin)
		local platform=darwin
		local ext=zip
		;;
	*)
		echo "ERROR: unsupported platform for etcd"
		exit 1
		;;
	esac

	case $(get_arch) in
	aarch64) local target=arm64 ;;
	x86_64) local target=amd64 ;;
	arm64) local target=arm64 ;;
	*)
		echo "ERROR: unsupported architecture for etcd"
		exit 1
		;;
	esac

	# SHA256 checksums for etcd v3.6.7 from official releases.
	local sha256
	case "${platform}-${target}" in
	linux-amd64) sha256="cf8af880c5a01ee5363cefa14a3e0cb7e5308dcf4ed17a6973099c9a7aee5a9a" ;;
	linux-arm64) sha256="ef5fc443cf7cc5b82738f3c28363704896551900af90a6d622cae740b5644270" ;;
	darwin-amd64) sha256="a9fe546f109b99733d1c797ff2598946b7ad155899e8458215da458f1d822ad5" ;;
	darwin-arm64) sha256="7a2d708e4f8aab9ba800676d2cfd4455367f5b67f9a012fad167f4b2b94524d7" ;;
	*)
		echo "ERROR: no checksum for etcd $platform-$target"
		exit 1
		;;
	esac

	local file="etcd-${version}-${platform}-${target}.${ext}"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q "https://github.com/etcd-io/etcd/releases/download/$version/$file"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	if [ "$ext" = "tar.gz" ]; then
		tar xzf "$file"
	else
		unzip "$file"
	fi
	rm "$file"
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
}

# Download and install consul, link consul binary into our root.
install_consul() {
	local version="$1"
	local dist="$2"

	case $(uname) in
	Linux) local platform=linux ;;
	Darwin) local platform=darwin ;;
	*)
		echo "ERROR: unsupported platform for consul"
		exit 1
		;;
	esac

	case $(get_arch) in
	aarch64) local target=arm64 ;;
	x86_64) local target=amd64 ;;
	arm64) local target=arm64 ;;
	*)
		echo "ERROR: unsupported architecture for consul"
		exit 1
		;;
	esac

	# SHA256 checksums for consul 1.11.4 from Vitess resources mirror.
	# Note: darwin checksums differ from official HashiCorp releases.
	local sha256
	case "${platform}_${target}" in
	linux_amd64) sha256="5155f6a3b7ff14d3671b0516f6b7310530b509a2b882b95b4fdf25f4219342c8" ;;
	linux_arm64) sha256="97dbf36500dcefbe463f070471602992d148cb2fe91db7e37319e1b9c809f1f0" ;;
	darwin_amd64) sha256="f00f81897ec0c608019a37dec243837ce0fd471b67401ea05be9a8b105d247ce" ;;
	darwin_arm64) sha256="22a87e88c9fd36f773ebd62b18f41dad5512e753769f5a385a7842a0b9364e0a" ;;
	*)
		echo "ERROR: no checksum for consul ${platform}_${target}"
		exit 1
		;;
	esac

	local file="consul_${version}_${platform}_${target}.zip"

	# This is how we'd download directly from source:
	# download_url=https://releases.hashicorp.com/consul
	# wget "${download_url}/${version}/${file}"
	"${VTROOT}/tools/wget-retry" -q "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"
	ln -snf "$dist/consul" "$VTROOT/bin/consul"
}

install_all() {
	echo "##local system details..."
	echo "##platform: $(uname) target:$(get_arch) OS: $OSTYPE"
	# protoc
	install_dep "protoc" "$PROTOC_VER" "$VTROOT/dist/vt-protoc-$PROTOC_VER" install_protoc

	# zk
	if [ "$BUILD_JAVA" == 1 ]; then
		install_dep "Zookeeper" "$ZK_VER" "$VTROOT/dist/vt-zookeeper-$ZK_VER" install_zookeeper
	fi

	# etcd
	install_dep "etcd" "$ETCD_VER" "$VTROOT/dist/etcd" install_etcd

	# consul
	if [ "$BUILD_CONSUL" == 1 ]; then
		install_dep "Consul" "$CONSUL_VER" "$VTROOT/dist/consul" install_consul
	fi

	echo
	echo "bootstrap finished - run 'make build' to compile"
}

install_all
