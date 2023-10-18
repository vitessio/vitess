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

source ./../dev.env

uninstall_protoc() {
    echo "Removing protoc..."
    local dist="$1"

    unlink "$dist/bin/protoc"
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


    unlink "$dist/etcd-${version}-${platform}-${target}/etcd"
    unlink "$dist/etcd-${version}-${platform}-${target}/etcdctl"
    rm -rf $dist
}

uninstall_consul() {
    echo "Removing consul..."
    local dist="$1"

    unlink "$dist/consul"
    rm -rf $dist
}
