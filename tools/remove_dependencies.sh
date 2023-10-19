#!/bin/bash

# Copyright 2023 The Vitess Authors.
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

set -euo pipefail

function fail() {
	echo "ERROR: ${1}"
	exit 1
}

BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_CONSUL=${BUILD_CONSUL:-1}
uname=$(uname)
get_arch=$(uname -m)

uninstall_protoc() {
    echo "Removing protoc..."
    local dist="$1"

    if [ -f "$dist/bin/protoc" ]; then
        unlink "$dist/bin/protoc"
        rm "$VTROOT/bin/protoc"
    fi
    if [[ "$(echo $dist | awk -F/ '{print $NF}')" == "vt-protoc-$PROTOC_VER" ]]; then
        rm -rf $dist
    fi
}

uninstall_zookeeper() {
    echo "Removing zookeeper..."
    local dist="$1"

    if [[ "$(echo $dist | awk -F/ '{print $NF}')" == "vt-zookeeper-$ZK_VER" ]]; then
        rm -rf $dist
    fi
}

uninstall_etcd() {
    echo "Removing etcd..."
    local version="$1"
    local dist="$2"

    case uname in
        Linux)  local platform=linux; local ext=tar.gz;;
        Darwin) local platform=darwin; local ext=zip;;
        *)   echo "Etcd not installed. Ignoring.."; return;;
    esac

    case get_arch in
        aarch64)  local target=arm64;;
        x86_64)  local target=amd64;;
        arm64)  local target=arm64;;
        *)   echo "Etcd not installed. Ignoring.."; return;;
    esac

    if [ -f "$dist/etcd-${version}-${platform}-${target}/etcd" ]; then
        unlink "$dist/etcd-${version}-${platform}-${target}/etcd"
        rm "$VTROOT/bin/etcd"
    fi
    if [ -f "$dist/etcd-${version}-${platform}-${target}/etcdctl" ]; then
        unlink "$dist/etcd-${version}-${platform}-${target}/etcdctl"
        rm "$VTROOT/bin/etcdctl"
    fi

    if [[ "$(echo $dist | awk -F/ '{print $NF}')" == "etcd" ]]; then
        rm -rf $dist
    fi
}

uninstall_consul() {
    echo "Removing consul..."
    local dist="$1"

    if [ -f "$dist/consul" ]; then
        unlink "$dist/consul"
        rm "$VTROOT/bin/consul"
    fi
    if [[ "$(echo $dist | awk -F/ '{print $NF}')" == "consul" ]]; then
        rm -rf $dist
    fi
}

uninstall_toxiproxy() {
    echo "Removing toxiproxy..."
    local dist="$1"

    case uname in
        Linux)  local platform=linux;;
        Darwin) local platform=darwin;;
        *)   echo "Toxiproxy not installed. Ignoring.."; return;;
    esac

    case get_arch in
        aarch64)  local target=arm64;;
        x86_64)  local target=amd64;;
        arm64)  local target=arm64;;
        *)   echo "Toxiproxy not installed. Ignoring.."; return;;
    esac

    file="toxiproxy-server-${platform}-${target}"

    if [ -f "$dist/$file" ]; then
        unlink "$dist/$file"
        rm "$VTROOT/bin/toxiproxy-server"
    fi
    if [[ "$(echo $dist | awk -F/ '{print $NF}')" == "toxiproxy" ]]; then
        rm -rf $dist
    fi
}

uninstall_all() {
    echo "## local system details..."
    echo "## platform: $uname target:$get_arch OS: $OSTYPE"

    # protoc
    uninstall_protoc "$VTROOT/dist/vt-protoc-$PROTOC_VER"

    # zk
    if [ "$BUILD_JAVA" == 1 ] ; then
        uninstall_zookeeper "$VTROOT/dist/vt-zookeeper-$ZK_VER"
    fi

    # etcd
    uninstall_etcd $ETCD_VER "$VTROOT/dist/etcd"

    # consul
    if [ "$BUILD_CONSUL" == 1 ] ; then
        uninstall_consul "$VTROOT/dist/consul"
    fi

    # toxiproxy
    uninstall_toxiproxy "$VTROOT/dist/toxiproxy"
}

uninstall_all
