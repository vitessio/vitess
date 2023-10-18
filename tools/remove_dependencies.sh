#!/bin/bash

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

# Remove tools and dependencies installed by "make tools"

get_arch() {
  uname -m
}

function fail() {
	echo "ERROR: ${1}"
	exit 1
}

BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_CONSUL=${BUILD_CONSUL:-1}
VTROOT="$PWD/../"

[[ "$(dirname "$0")" = "." ]] || fail "remove_dependencies.sh must be run from its current directory"

uninstall_protoc() {
    echo "Removing protoc..."
    local dist="$1"

    if [ -f $dist ]; then
        unlink "$dist/bin/protoc"
        rm "$VTROOT/bin/protoc"
    fi
    rm -rf $dist
}

uninstall_zookeeper() {
    echo "Removing zookeeper..."
    local dist="$1"

    rm -rf $dist
}

uninstall_etcd() {
    echo "Removing etcd..."
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

    if [ -f "$dist/etcd-${version}-${platform}-${target}/etcd" ]; then
        unlink "$dist/etcd-${version}-${platform}-${target}/etcd"
        rm "$VTROOT/bin/etcd"
    fi
    if [ -f "$dist/etcd-${version}-${platform}-${target}/etcdctl" ]; then
        unlink "$dist/etcd-${version}-${platform}-${target}/etcdctl"
        rm "$VTROOT/bin/etcdctl"
    fi
    rm -rf $dist
}

uninstall_consul() {
    echo "Removing consul..."
    local dist="$1"

    if [ -f "$dist/consul" ]; then
        unlink "$dist/consul"
        rm "$VTROOT/bin/consul"
    fi
    rm -rf $dist
}

uninstall_toxiproxy() {
    echo "Removing toxiproxy..."
    local dist="$1"

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

    file="toxiproxy-server-${platform}-${target}"

    if [ -f "$dist/$file" ]; then
        unlink "$dist/$file"
        rm "$VTROOT/bin/toxiproxy-server"
    fi
    rm -rf $dist
}

uninstall_all() {
    echo "## local system details..."
    echo "## platform: $(uname) target:$(get_arch) OS: $os"

    # protoc
    protoc_ver=21.3
    uninstall_protoc "$VTROOT/dist/vt-protoc-$protoc_ver"

    # zk
    zk_ver=${ZK_VERSION:-3.8.0}
    if [ "$BUILD_JAVA" == 1 ] ; then
        uninstall_zookeeper "$VTROOT/dist/vt-zookeeper-$zk_ver"
    fi

    # etcd
    uninstall_etcd "v3.5.6" "$VTROOT/dist/etcd"

    # consul
    if [ "$BUILD_CONSUL" == 1 ] ; then
        uninstall_consul "$VTROOT/dist/consul"
    fi

    # toxiproxy
    uninstall_toxiproxy "$VTROOT/dist/toxiproxy"
}

uninstall_all
