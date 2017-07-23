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

# This is a wrapper script that installs and runs the example
# client for the JDBC interface.

set -e

script_root=`dirname "${BASH_SOURCE}"`

# When this script is run with the argument "--enable-tls", then it will connect to VTGate using TLS with
# client authentication.  This option depends upon the "vtgate-up.sh" script having also been run with
# a corresponding "--enable-tls" argument.
optional_tls_args=''
if [ "$1" = "--enable-tls" ];
then
	echo "Enabling TLS with client authentication"
	cert_dir=$VTDATAROOT/tls
	rm -f $cert_dir/ca-trustStore.jks
	rm -f $cert_dir/client-keyStore.jks

    # Create CA trustStore
    openssl x509 -outform der -in $cert_dir/ca-cert.pem -out $cert_dir/ca-cert.der
    keytool -import -alias cacert -keystore $cert_dir/ca-trustStore.jks -file $cert_dir/ca-cert.der -storepass passwd -trustcacerts -noprompt

    # Create client-side signed cert keyStore
    openssl pkcs12 -export -in $cert_dir/client-cert.pem -inkey $cert_dir/client-key.pem -out $cert_dir/client-key.p12 -name cert -CAfile $cert_dir/ca-cert.pem -caname root -passout pass:passwd
    keytool -importkeystore -deststorepass passwd -destkeystore $cert_dir/client-keyStore.jks -srckeystore $cert_dir/client-key.p12 -srcstoretype PKCS12 -alias cert -srcstorepass passwd

    optional_tls_args="?useSSL=true&keyStore=$cert_dir/client-keyStore.jks&keyStorePassword=passwd&keyAlias=cert&trustStore=$cert_dir/ca-trustStore.jks&trustStorePassword=passwd&trustAlias=cacert"
    echo $optional_tls_args
fi

# We have to install the "example" module first because Maven cannot resolve
# them when we run "exec:java". See also: http://stackoverflow.com/questions/11091311/maven-execjava-goal-on-a-multi-module-project
# Install only "example". See also: http://stackoverflow.com/questions/1114026/maven-modules-building-a-single-specific-module
mvn -f $script_root/../../java/pom.xml -pl example -am install -DskipTests
mvn -f $script_root/../../java/example/pom.xml exec:java \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.mainClass="io.vitess.example.VitessJDBCExample" \
    -Dexec.args="localhost:15991$optional_tls_args"
