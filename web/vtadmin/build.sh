#!/bin/bash

function output() {
  echo -e "$@"
}

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../../build.env"
web_dir="${script_dir}"

# Download nvm and node
if [[ -z ${NVM_DIR} ]]; then
    export NVM_DIR="$HOME/.nvm"
fi

if [[ -z ${NODE_VERSION} ]]; then
    export NODE_VERSION="16"
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