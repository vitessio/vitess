#!/bin/bash

# Copyright 2017 Google Inc.
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

# This is an example script that starts a single vtgate.

set -e

cell=${CELL:-'test'}
web_port=15001
grpc_port=15991
mysql_server_port=15306
mysql_server_socket_path="/tmp/mysql.sock"

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# When this script is run with the argument "--enable-tls", then it will generate signed certs and
# configure VTGate to accept only TLS connections with client authentication.  This allows for end-to-end
# SSL testing (see also the "client_jdbc.sh" script).
optional_tls_args=''
if [ "$1" = "--enable-tls" ];
then
	echo "Enabling TLS with client authentication"
	config_dir=../../java/grpc-client/src/test/resources
	cert_dir=$VTDATAROOT/tls
	rm -Rf $cert_dir
	mkdir -p $cert_dir

    # Create CA
    openssl genrsa -out $cert_dir/ca-key.pem
    openssl req -new -x509 -nodes -days 3600 -batch -config $config_dir/ca.config -key $cert_dir/ca-key.pem -out $cert_dir/ca-cert.pem

    # Create server-side signed cert
    openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config $config_dir/cert.config -keyout $cert_dir/server-key.pem -out $cert_dir/server-req.pem
    openssl x509 -req -in $cert_dir/server-req.pem -days 3600 -CA $cert_dir/ca-cert.pem -CAkey $cert_dir/ca-key.pem -set_serial 01 -out $cert_dir/server-cert.pem

    # Create client-side signed cert
    openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config $config_dir/cert.config -keyout $cert_dir/client-key.pem -out $cert_dir/client-req.pem
    openssl x509 -req -in $cert_dir/client-req.pem -days 3600 -CA $cert_dir/ca-cert.pem -CAkey $cert_dir/ca-key.pem -set_serial 02 -out $cert_dir/client-cert.pem

    optional_tls_args="-grpc_cert $cert_dir/server-cert.pem -grpc_key $cert_dir/server-key.pem -grpc_ca $cert_dir/ca-cert.pem"
fi

optional_auth_args='-mysql_auth_server_impl none'
optional_grpc_auth_args=''
if [ "$1" = "--enable-grpc-static-auth" ];
then
	  echo "Enabling Auth with static authentication in grpc"
    optional_grpc_auth_args='-grpc_auth_static_client_creds ./grpc_static_client_auth.json'
fi

if [ "$1" = "--enable-mysql-static-auth" ];
then
    echo "Enabling Auth with mysql static authentication"
    optional_auth_args='-mysql_auth_server_static_file ./mysql_auth_server_static_creds.json'
fi

# Start vtgate.
# shellcheck disable=SC2086
$VTROOT/bin/vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -log_queries_to_file $VTDATAROOT/tmp/vtgate_querylog.txt \
  -port $web_port \
  -grpc_port $grpc_port \
  -mysql_server_port $mysql_server_port \
  -mysql_server_socket_path $mysql_server_socket_path \
  -cell $cell \
  -cells_to_watch $cell \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  $optional_auth_args \
  $optional_grpc_auth_args \
  $optional_tls_args \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://$hostname:$web_port/debug/status"

disown -a

