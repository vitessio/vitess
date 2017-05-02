#!/bin/bash

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
