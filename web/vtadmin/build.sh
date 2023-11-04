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

function output() {
  echo -e "$@"
}

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../../build.env"
web_dir="${script_dir}"

vtadmin_api_port=14200

# Download nvm and node
if [[ -z ${NVM_DIR} ]]; then
    export NVM_DIR="$HOME/.nvm"
fi

if [[ -z ${NODE_VERSION} ]]; then
    export NODE_VERSION="18.16.0"
fi

output "\nInstalling nvm...\n"

if [ -d "$NVM_DIR" ]; then
  output "\033[1;32mnvm is already installed!\033[0m"
else
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash && output "\033[1;32mnvm is installed!\033[0m" || fail "\033[1;32mnvm failed to install!\033[0m"
fi

source "$NVM_DIR/nvm.sh"

output "\nConfiguring Node.js $NODE_VERSION\n"
nvm install "$NODE_VERSION" || fail "Could not install and use nvm $NODE_VERSION."

npm --prefix "$web_dir" --silent install

export PATH=$PATH:$web_dir/node_modules/.bin/

VITE_VTADMIN_API_ADDRESS="http://${hostname}:${vtadmin_api_port}" \
  VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS="true" \
  npm run --prefix "$web_dir" build
