#!/bin/bash
# Copyright 2016 The Kubernetes Authors.
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

# This is forked from:
# https://github.com/kubernetes/contrib/blob/master/micro-demos/util.sh

readonly  reset=$(tput sgr0)
readonly  green=$(tput bold; tput setaf 2)
readonly yellow=$(tput bold; tput setaf 3)
readonly   blue=$(tput bold; tput setaf 6)
readonly timeout=$(if [ "$(uname)" == "Darwin" ]; then echo "1"; else echo "0.1"; fi) 

function desc() {
    maybe_first_prompt
    echo "$blue# $@$reset"
    prompt
}

function prompt() {
    echo -n "$yellow\$ $reset"
}

started=""
function maybe_first_prompt() {
    if [ -z "$started" ]; then
        prompt
        started=true
    fi
}

# After a `run` this variable will hold the stdout of the command that was run.
# If the command was interactive, this will likely be garbage.
DEMO_RUN_STDOUT=""

function run() {
    maybe_first_prompt
    rate=200
    if [ -n "$DEMO_RUN_FAST" ]; then
      rate=1000
    fi
    echo -n "$green$1$reset" | pv -qL $rate
    if [ -z "$DEMO_AUTO_RUN" ]; then
      read skip
      if [ -n "$skip" ]; then
        echo "Skipping..."
        return 0
      fi
    fi
    OFILE="$(mktemp -t $(basename $0).XXXXXX)"
    script -eq -c "$1" -f "$OFILE"
    r=$?
    read -d '' -t "${timeout}" -n 10000 # clear stdin
    prompt
    if [ -z "$DEMO_AUTO_RUN" ]; then
      read -s
    fi
    DEMO_RUN_STDOUT="$(tail -n +2 $OFILE | sed 's/\r//g')"
    return $r
}

function relative() {
    for arg; do
        echo "$(realpath $(dirname $(which $0)))/$arg" | sed "s|$(realpath $(pwd))|.|"
    done
}

SSH_NODE=$(kubectl get nodes | tail -1 | cut -f1 -d' ')

trap "echo" EXIT

