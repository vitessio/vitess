#!/usr/bin/env bash
#
# See https://vitess.io/docs/tutorials/vagrant/ 
# for more info
#

ulimit -n 10000
export MYSQL_FLAVOR=MySQL56
export VT_MYSQL_ROOT=/usr

printf "\nBuilding Vitess...\n"

# Stopping on errors makes them easier to see
set -e

# This is just to make sure the vm can write into these directories
sudo chown "$(whoami)":"$(whoami)" /vagrant
sudo chown "$(whoami)":"$(whoami)" /vagrant/src
cd "$VITESS_WORKSPACE"

# open-jdk version that we are using in the VM needs this flag, otherwise we will fail to build ZK
export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"

./bootstrap.sh
# shellcheck disable=SC1091
source dev.env
# shellcheck disable=SC1091
source /vagrant/dist/grpc/usr/local/bin/activate
pip install mysqlclient
make build

printf "\nBuild completed\n\n"
