/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package tlstest contains utility methods to create test certificates.
// It is not meant to be used in production.
package tlstest

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	"vitess.io/vitess/go/vt/log"
)

const (
	// CA is the name of the CA toplevel cert.
	CA = "ca"

	caConfig = `
[ req ]
 default_bits           = 4096
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 x509_extensions       = req_ext
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Vitessio
 OU                     = Vitess
 CN                     = CA
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
[ req_ext ]
 basicConstraints       = CA:TRUE

`

	certConfig = `
[ req ]
 default_bits           = 4096
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 req_extensions        = req_ext
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Vitessio
 OU                     = Vitess
 CN                     = %s
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
[ req_ext ]
 basicConstraints       = CA:TRUE
 subjectAltName         = @alternate_names
 extendedKeyUsage       =serverAuth,clientAuth
[ alternate_names ]
 DNS.1                  = localhost
 DNS.2                  = 127.0.0.1
 DNS.3                  = %s
`
)

// openssl runs the openssl binary with the provided command.
func openssl(argv ...string) {
	cmd := exec.Command("openssl", argv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("openssl %v failed: %v", argv, err)
	}
	if len(output) > 0 {
		log.Infof("openssl %v returned:\n%v", argv, string(output))
	}
}

// CreateCA creates the toplevel 'ca' certificate and key, and places it
// in the provided directory. Temporary files are also created in that
// directory.
func CreateCA(root string) {
	log.Infof("Creating test root CA in %v", root)
	key := path.Join(root, "ca-key.pem")
	cert := path.Join(root, "ca-cert.pem")
	openssl("genrsa", "-out", key)

	config := path.Join(root, "ca.config")
	if err := ioutil.WriteFile(config, []byte(caConfig), os.ModePerm); err != nil {
		log.Fatalf("cannot write file %v: %v", config, err)
	}
	openssl("req", "-new", "-x509", "-nodes", "-days", "3600", "-batch",
		"-config", config,
		"-key", key,
		"-out", cert)
}

// CreateSignedCert creates a new certificate signed by the provided parent,
// with the provided serial number, name and common name.
// name is the file name to use. Common Name is the certificate common name.
func CreateSignedCert(root, parent, serial, name, commonName string) {
	log.Infof("Creating signed cert and key %v", commonName)
	caKey := path.Join(root, parent+"-key.pem")
	caCert := path.Join(root, parent+"-cert.pem")
	key := path.Join(root, name+"-key.pem")
	cert := path.Join(root, name+"-cert.pem")
	req := path.Join(root, name+"-req.pem")

	config := path.Join(root, name+".config")
	if err := ioutil.WriteFile(config, []byte(fmt.Sprintf(certConfig, commonName, commonName)), os.ModePerm); err != nil {
		log.Fatalf("cannot write file %v: %v", config, err)
	}
	openssl("req", "-newkey", "rsa:2048", "-days", "3600", "-nodes",
		"-batch",
		"-config", config,
		"-keyout", key, "-out", req)
	openssl("rsa", "-in", key, "-out", key)
	openssl("x509", "-req",
		"-in", req,
		"-days", "3600",
		"-CA", caCert,
		"-CAkey", caKey,
		"-set_serial", serial,
		"-extensions", "req_ext",
		"-extfile", config,
		"-out", cert)
}

type ClientServerKeyPairs struct {
	ServerCert string
	ServerKey  string
	ServerCA   string
	ServerName string
	ClientCert string
	ClientKey  string
	ClientCA   string
}

var serialCounter = 0

func CreateClientServerCertPairs(root string) ClientServerKeyPairs {
	// Create the certs and configs.
	CreateCA(root)

	serverSerial := fmt.Sprintf("%03d", serialCounter*2+1)
	clientSerial := fmt.Sprintf("%03d", serialCounter*2+2)

	serialCounter = serialCounter + 1

	serverName := fmt.Sprintf("server-%s", serverSerial)
	serverCACommonName := fmt.Sprintf("Server %s CA", serverSerial)
	serverCertName := fmt.Sprintf("server-instance-%s", serverSerial)
	serverCertCommonName := fmt.Sprintf("server%s.example.com", serverSerial)

	clientName := fmt.Sprintf("clients-%s", serverSerial)
	clientCACommonName := fmt.Sprintf("Clients %s CA", serverSerial)
	clientCertName := fmt.Sprintf("client-instance-%s", serverSerial)
	clientCertCommonName := fmt.Sprintf("Client Instance %s", serverSerial)

	CreateSignedCert(root, CA, serverSerial, serverName, serverCACommonName)
	CreateSignedCert(root, serverName, serverSerial, serverCertName, serverCertCommonName)

	CreateSignedCert(root, CA, clientSerial, clientName, clientCACommonName)
	CreateSignedCert(root, clientName, serverSerial, clientCertName, clientCertCommonName)

	return ClientServerKeyPairs{
		ServerCert: path.Join(root, fmt.Sprintf("%s-cert.pem", serverCertName)),
		ServerKey:  path.Join(root, fmt.Sprintf("%s-key.pem", serverCertName)),
		ServerCA:   path.Join(root, fmt.Sprintf("%s-cert.pem", serverName)),
		ClientCert: path.Join(root, fmt.Sprintf("%s-cert.pem", clientCertName)),
		ClientKey:  path.Join(root, fmt.Sprintf("%s-key.pem", clientCertName)),
		ClientCA:   path.Join(root, fmt.Sprintf("%s-cert.pem", clientName)),
		ServerName: serverCertCommonName,
	}
}
