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
# Maven is off by default: docker/test/run.sh turns it on in the test
# containers, and the go_cache_warm workflow in its Maven job.
BUILD_MAVEN=${BUILD_MAVEN:-0}

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

  echo "$version" > "$version_file"
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

# Downloads are kept here, outside the directories that install_dep wipes,
# so that they survive a re-install and can be cached (CI does).
DOWNLOAD_CACHE="$VTROOT/dist/download-cache"

# checksum_matches reports whether a file exists and has the expected checksum.
#
# Usage: checksum_matches <256|512> <file> <expected_checksum>
checksum_matches() {
  local bits="$1"
  local file="$2"
  local expected="$3"

  [[ -f "$file" ]] || return 1
  if command -v "sha${bits}sum" &>/dev/null; then
    echo "$expected  $file" | "sha${bits}sum" -c - &>/dev/null
  elif command -v shasum &>/dev/null; then
    echo "$expected  $file" | shasum -a "$bits" -c - &>/dev/null
  else
    fail "Neither sha${bits}sum nor shasum found. Cannot verify checksum."
  fi
}

# download_verified makes sure that $DOWNLOAD_CACHE/<file> exists and has the
# expected checksum. A cached file is used only if its checksum matches;
# otherwise it is downloaded again from the first URL that works. Only a
# download that passes the checksum is moved into the cache, with a rename that
# replaces the cached file atomically. The cached file is never deleted first:
# the docker test containers share the cache, and another one may have just
# published it and be about to extract it.
#
# Usage: download_verified <file> <256|512> <expected_checksum> <url>...
download_verified() {
  local file="$1"
  local bits="$2"
  local expected="$3"
  shift 3

  local cached="$DOWNLOAD_CACHE/$file"
  if checksum_matches "$bits" "$cached" "$expected"; then
    echo "OK: using cached $cached"
    return
  fi

  mkdir -p "$DOWNLOAD_CACHE"
  # A unique name, as the docker test containers share the cache.
  local partial
  partial="$(mktemp "$cached.XXXXXX")"
  local url
  for url in "$@"; do
    # Tries must be bounded: wget-retry retries a 404 forever by default and
    # the next URL would never be tried.
    if "${VTROOT}/tools/wget-retry" -q -t 3 -O "$partial" "$url"; then
      if checksum_matches "$bits" "$partial" "$expected"; then
        # mktemp makes the file private, but the docker builds and test
        # containers read it as another user.
        chmod 644 "$partial"
        mv "$partial" "$cached"
        echo "OK: SHA${bits} checksum verified for $cached"
        return
      fi
      rm -f "$partial"
      fail "SHA${bits} checksum verification failed for $file downloaded from $url"
    fi
  done
  rm -f "$partial"
  fail "Could not download $file"
}

# Install protoc.
install_protoc() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=osx;;
    *) echo "ERROR: unsupported platform for protoc"; exit 1;;
  esac

  case $(get_arch) in
      aarch64)  local target=aarch_64;;
      x86_64)  local target=x86_64;;
      arm64) case "$platform" in
          osx) local target=aarch_64;;
          *) echo "ERROR: unsupported architecture for protoc"; exit 1;;
      esac;;
      *)   echo "ERROR: unsupported architecture for protoc"; exit 1;;
  esac

  # SHA256 checksums for protoc v21.3 from official releases.
  local sha256
  case "${platform}-${target}" in
    linux-x86_64) sha256="214b670884972d09a1ccf7b7b4c65a12bdd73d55ebea6b5207b5eb2333ae32ec";;
    linux-aarch_64) sha256="e22ad6908e197ac326a02ddabc49046846b64d051f3bef16f5d3afd50ffdec18";;
    osx-x86_64) sha256="b988e06fed5279865445978efd97ec92990ca24b0fe16c04aafe85d6cee71eeb";;
    osx-aarch_64) sha256="3f1b2a59ba111e3227adf47e5513b3ea9133d9621dc5df70cd1ffed6dc756877";;
    *) echo "ERROR: no checksum for protoc $platform-$target"; exit 1;;
  esac

  local file="protoc-${version}-${platform}-${target}.zip"

  # This is how we'd download from the Vitess resources mirror:
  # "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  download_verified "$file" 256 "$sha256" "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
  unzip "$DOWNLOAD_CACHE/$file"

  ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
}


# Install Zookeeper.
install_zookeeper() {
  local version="$1"
  local dist="$2"
  zk="zookeeper-$version"
  local file="apache-${zk}.tar.gz"

  # SHA512 checksum for the Zookeeper 3.8.0 source release from Apache archives.
  local sha512="0bfaf823af115d3d8e589d69312658df359a8c740996acf3c7fc392c8912a588507189831459ede89b0cfd203fac96d3bb2b3f258b894fedc034d7f1331f6d0a"

  # This is how we'd download directly from source:
  # wget "https://dlcdn.apache.org/zookeeper/$zk/${file}"
  download_verified "$file" 512 "$sha512" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  tar -xzf "$DOWNLOAD_CACHE/$file"
  mvn -q -f $dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/pom.xml clean install -P fatjar -DskipTests
  mkdir -p $dist/lib
  cp "$dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/target/$zk-fatjar.jar" "$dist/lib/$zk-fatjar.jar"
  rm -rf "$dist/apache-$zk"
}

# Install Maven.
install_maven() {
  local version="$1"
  local file="apache-maven-${version}-bin.tar.gz"

  # SHA512 checksum for Maven 3.9.16 from Apache downloads.
  local sha512="831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6"

  # Maven Central keeps every Maven release, unlike dlcdn.apache.org, and
  # archive.apache.org keeps timing out from the CI runners.
  download_verified "$file" 512 "$sha512" \
    "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${version}/${file}"
  tar -xzf "$DOWNLOAD_CACHE/$file" --strip-components=1
}


# Download and install etcd, link etcd binary into our root.
install_etcd() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux; local ext=tar.gz;;
    Darwin) local platform=darwin; local ext=zip;;
    *)   echo "ERROR: unsupported platform for etcd"; exit 1;;
  esac

  case $(get_arch) in
      aarch64)  local target=arm64;;
      x86_64)  local target=amd64;;
      arm64)  local target=arm64;;
      *)   echo "ERROR: unsupported architecture for etcd"; exit 1;;
  esac

  # SHA256 checksums for etcd v3.5.25 from official releases.
  local sha256
  case "${platform}-${target}" in
    linux-amd64) sha256="168af82b59772e1811a9af7b358d42f5c6df44e0d9767afb006ecf12c4bbd607";;
    linux-arm64) sha256="419dce0b679df31cc45201ef2449b7a6a48e9d241af01741957c9ac86a35badc";;
    darwin-amd64) sha256="9ed3e9128ebae6cbd5b4d758a5b67fb10718c03516cc215f50fb570dc51df7b9";;
    darwin-arm64) sha256="feff53187f00c0029a6dd83441c3d011fdde0107588f547aa88567b6cc21de41";;
    *) echo "ERROR: no checksum for etcd $platform-$target"; exit 1;;
  esac

  local file="etcd-${version}-${platform}-${target}.${ext}"

  # This is how we'd download from the Vitess resources mirror:
  # "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  download_verified "$file" 256 "$sha256" "https://github.com/etcd-io/etcd/releases/download/$version/$file"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$DOWNLOAD_CACHE/$file"
  else
    unzip "$DOWNLOAD_CACHE/$file"
  fi
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
}

# Download and install consul, link consul binary into our root.
install_consul() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
    *)   echo "ERROR: unsupported platform for consul"; exit 1;;
  esac

  case $(get_arch) in
    aarch64)  local target=arm64;;
    x86_64)  local target=amd64;;
    arm64)  local target=arm64;;
    *)   echo "ERROR: unsupported architecture for consul"; exit 1;;
  esac

  # SHA256 checksums for consul 1.11.4 from Vitess resources mirror.
  # Note: darwin checksums differ from official HashiCorp releases.
  local sha256
  case "${platform}_${target}" in
    linux_amd64) sha256="5155f6a3b7ff14d3671b0516f6b7310530b509a2b882b95b4fdf25f4219342c8";;
    linux_arm64) sha256="97dbf36500dcefbe463f070471602992d148cb2fe91db7e37319e1b9c809f1f0";;
    darwin_amd64) sha256="f00f81897ec0c608019a37dec243837ce0fd471b67401ea05be9a8b105d247ce";;
    darwin_arm64) sha256="22a87e88c9fd36f773ebd62b18f41dad5512e753769f5a385a7842a0b9364e0a";;
    *) echo "ERROR: no checksum for consul ${platform}_${target}"; exit 1;;
  esac

  local file="consul_${version}_${platform}_${target}.zip"

  # This is how we'd download directly from source:
  # download_url=https://releases.hashicorp.com/consul
  # wget "${download_url}/${version}/${file}"
  download_verified "$file" 256 "$sha256" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  unzip "$DOWNLOAD_CACHE/$file"
  ln -snf "$dist/consul" "$VTROOT/bin/consul"
}


# Download and install toxiproxy, link toxiproxy binary into our root.
install_toxiproxy() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
    *)   echo "WARNING: unsupported platform. Some tests that rely on toxiproxy will not function."; return;;
  esac

  case $(get_arch) in
    aarch64)  local target=arm64;;
    x86_64)  local target=amd64;;
    arm64)  local target=arm64;;
    *)   echo "WARNING: unsupported architecture. Some tests that rely on toxiproxy will not function."; return;;
  esac

  # SHA256 checksums for toxiproxy v2.7.0 from official releases.
  local sha256
  case "${platform}-${target}" in
    linux-amd64) sha256="4957d7e8949ecc54f798fdfc8901c022ff24cd4394493be9e321a3e3d4556ac0";;
    linux-arm64) sha256="7f63da3a5b15a8db31cf05379d09fda9bce862288c7f70912c78237ce030b0c9";;
    darwin-amd64) sha256="8f439c3abea34c20ef615aa46368e883ec7e1c9c904070f3be7f05f90f9ac51b";;
    darwin-arm64) sha256="619148a5af477aa83f642e7ed0f5de05f005ac493fae3ec06ece00a22df4a2cb";;
    *) echo "WARNING: no checksum for toxiproxy $platform-$target. Some tests that rely on toxiproxy will not function."; return;;
  esac

  # This is how we'd download directly from source:
  local file="toxiproxy-server-${platform}-${target}"
  download_verified "$file" 256 "$sha256" "https://github.com/Shopify/toxiproxy/releases/download/$version/$file"
  cp "$DOWNLOAD_CACHE/$file" "$dist/$file"
  chmod +x "$dist/$file"
  ln -snf "$dist/$file" "$VTROOT/bin/toxiproxy-server"
}

install_all() {
  echo "##local system details..."
  echo "##platform: $(uname) target:$(get_arch) OS: $OSTYPE"
  # protoc
  install_dep "protoc" "$PROTOC_VER" "$VTROOT/dist/vt-protoc-$PROTOC_VER" install_protoc

  # zk
  if [ "$BUILD_JAVA" == 1 ] ; then
    install_dep "Zookeeper" "$ZK_VER" "$VTROOT/dist/vt-zookeeper-$ZK_VER" install_zookeeper
  fi

  # etcd
  install_dep "etcd" "$ETCD_VER" "$VTROOT/dist/etcd" install_etcd

  # consul
  if [ "$BUILD_CONSUL" == 1 ] ; then
    install_dep "Consul" "$CONSUL_VER" "$VTROOT/dist/consul" install_consul
  fi

  # toxiproxy
  install_dep "toxiproxy" "$TOXIPROXY_VER" "$VTROOT/dist/toxiproxy" install_toxiproxy

  # Maven, on the PATH of the bootstrap image as dist/maven/bin.
  if [ "$BUILD_MAVEN" == 1 ]; then
    install_dep "Maven" "$MAVEN_VER" "$VTROOT/dist/maven" install_maven
  fi

  echo
  echo "bootstrap finished - run 'make build' to compile"
}

install_all
