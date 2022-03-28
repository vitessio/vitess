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
	"os"
	"os/exec"
	"path"

	"vitess.io/vitess/go/vt/log"
)

const (
	// CA is the name of the CA toplevel cert.
	CA = "ca"

	caConfigTemplate = `
[ ca ]
default_ca = default_ca

[ default_ca ]
database = %s
default_md = default
default_crl_days = 30

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
		if len(output) > 0 {
			log.Errorf("openssl %v returned:\n%v", argv, string(output))
		}
		log.Fatalf("openssl %v failed: %v", argv, err)
	}
}

// createKeyDBAndCAConfig creates a key database and ca config file
// for the passed in CA (possibly an intermediate CA)
func createKeyDBAndCAConfig(root, parent string) {
	databasePath := path.Join(root, parent+"-keys.db")
	if _, err := os.Stat(databasePath); os.IsNotExist(err) {
		if err := os.WriteFile(databasePath, []byte{}, os.ModePerm); err != nil {
			log.Fatalf("cannot write file %v: %v", databasePath, err)
		}
	}

	config := path.Join(root, parent+"-ca.config")
	if _, err := os.Stat(config); os.IsNotExist(err) {
		if err := os.WriteFile(config, []byte(fmt.Sprintf(caConfigTemplate, databasePath)), os.ModePerm); err != nil {
			log.Fatalf("cannot write file %v: %v", config, err)
		}
	}
}

// CreateCA creates the toplevel 'ca' certificate and key, and places it
// in the provided directory. Temporary files are also created in that
// directory.
func CreateCA(root string) {
	log.Infof("Creating test root CA in %v", root)
	key := path.Join(root, "ca-key.pem")
	cert := path.Join(root, "ca-cert.pem")
	config := path.Join(root, "ca-ca.config")
	createKeyDBAndCAConfig(root, "ca")

	openssl("genrsa", "-out", key)

	openssl("req",
		"-new",
		"-x509",
		"-days", "3600",
		"-nodes",
		"-batch",
		"-sha256",
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
	if err := os.WriteFile(config, []byte(fmt.Sprintf(certConfig, commonName, commonName)), os.ModePerm); err != nil {
		log.Fatalf("cannot write file %v: %v", config, err)
	}
	openssl("req",
		"-newkey", "rsa:2048",
		"-days", "3600",
		"-nodes",
		"-batch",
		"-sha256",
		"-config", config,
		"-keyout", key,
		"-out", req)
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

// CreateCRL creates a new empty certificate revocation list
// for the provided parent
func CreateCRL(root, parent string) {
	log.Infof("Creating CRL for root CA in %v", root)
	caKey := path.Join(root, parent+"-key.pem")
	caCert := path.Join(root, parent+"-cert.pem")
	configPath := path.Join(root, parent+"-ca.config")
	crlPath := path.Join(root, parent+"-crl.pem")
	createKeyDBAndCAConfig(root, parent)

	openssl("ca", "-gencrl",
		"-keyfile", caKey,
		"-cert", caCert,
		"-config", configPath,
		"-out",
		crlPath,
	)
}

// RevokeCertAndRegenerateCRL revokes a provided certificate under the
// provided parent CA and regenerates the CRL file for that parent
func RevokeCertAndRegenerateCRL(root, parent, name string) {
	log.Infof("Revoking certificate %s", name)
	caKey := path.Join(root, parent+"-key.pem")
	caCert := path.Join(root, parent+"-cert.pem")
	cert := path.Join(root, name+"-cert.pem")
	configPath := path.Join(root, parent+"-ca.config")
	createKeyDBAndCAConfig(root, parent)

	openssl("ca", "-revoke", cert,
		"-keyfile", caKey,
		"-cert", caCert,
		"-config", configPath,
	)

	CreateCRL(root, parent)
}

// ClientServerKeyPairs is used in tests
type ClientServerKeyPairs struct {
	ServerCert        string
	ServerKey         string
	ServerCA          string
	ServerName        string
	ServerCRL         string
	RevokedServerCert string
	RevokedServerKey  string
	RevokedServerName string
	ClientCert        string
	ClientKey         string
	ClientCA          string
	ClientCRL         string
	RevokedClientCert string
	RevokedClientKey  string
	RevokedClientName string
	CombinedCRL       string
}

var serialCounter = 0

// CreateClientServerCertPairs creates certificate pairs for use in test
func CreateClientServerCertPairs(root string) ClientServerKeyPairs {
	// Create the certs and configs.
	CreateCA(root)

	serverCASerial := fmt.Sprintf("%03d", serialCounter*2+1)
	serverSerial := fmt.Sprintf("%03d", serialCounter*2+3)
	revokedServerSerial := fmt.Sprintf("%03d", serialCounter*2+5)
	clientCASerial := fmt.Sprintf("%03d", serialCounter*2+2)
	clientCertSerial := fmt.Sprintf("%03d", serialCounter*2+4)
	revokedClientSerial := fmt.Sprintf("%03d", serialCounter*2+6)

	serialCounter = serialCounter + 3

	serverCAName := fmt.Sprintf("servers-ca-%s", serverCASerial)
	serverCACommonName := fmt.Sprintf("Servers %s CA", serverCASerial)
	serverCertName := fmt.Sprintf("server-instance-%s", serverSerial)
	serverCertCommonName := fmt.Sprintf("server%s.example.com", serverSerial)
	revokedServerCertName := fmt.Sprintf("server-instance-%s", revokedServerSerial)
	revokedServerCertCommonName := fmt.Sprintf("server%s.example.com", revokedServerSerial)

	clientCAName := fmt.Sprintf("clients-ca-%s", clientCASerial)
	clientCACommonName := fmt.Sprintf("Clients %s CA", clientCASerial)
	clientCertName := fmt.Sprintf("client-instance-%s", clientCertSerial)
	clientCertCommonName := fmt.Sprintf("client%s.example.com", clientCertSerial)
	revokedClientCertName := fmt.Sprintf("client-instance-%s", revokedClientSerial)
	revokedClientCertCommonName := fmt.Sprintf("client%s.example.com", revokedClientSerial)

	CreateSignedCert(root, CA, serverCASerial, serverCAName, serverCACommonName)
	CreateSignedCert(root, serverCAName, serverSerial, serverCertName, serverCertCommonName)
	CreateSignedCert(root, serverCAName, revokedServerSerial, revokedServerCertName, revokedServerCertCommonName)
	RevokeCertAndRegenerateCRL(root, serverCAName, revokedServerCertName)

	CreateSignedCert(root, CA, clientCASerial, clientCAName, clientCACommonName)
	CreateSignedCert(root, clientCAName, clientCertSerial, clientCertName, clientCertCommonName)
	CreateSignedCert(root, clientCAName, revokedClientSerial, revokedClientCertName, revokedClientCertCommonName)
	RevokeCertAndRegenerateCRL(root, clientCAName, revokedClientCertName)

	serverCRLPath := path.Join(root, fmt.Sprintf("%s-crl.pem", serverCAName))
	clientCRLPath := path.Join(root, fmt.Sprintf("%s-crl.pem", clientCAName))
	combinedCRLPath := path.Join(root, fmt.Sprintf("%s-%s-combined-crl.pem", serverCAName, clientCAName))

	serverCRLBytes, err := os.ReadFile(serverCRLPath)
	if err != nil {
		log.Fatalf("Could not read server CRL file")
	}

	clientCRLBytes, err := os.ReadFile(clientCRLPath)
	if err != nil {
		log.Fatalf("Could not read client CRL file")
	}

	err = os.WriteFile(combinedCRLPath, append(serverCRLBytes, clientCRLBytes...), 0777)
	if err != nil {
		log.Fatalf("Could not write combined CRL file")
	}

	return ClientServerKeyPairs{
		ServerCert:        path.Join(root, fmt.Sprintf("%s-cert.pem", serverCertName)),
		ServerKey:         path.Join(root, fmt.Sprintf("%s-key.pem", serverCertName)),
		ServerCA:          path.Join(root, fmt.Sprintf("%s-cert.pem", serverCAName)),
		ServerCRL:         serverCRLPath,
		RevokedServerCert: path.Join(root, fmt.Sprintf("%s-cert.pem", revokedServerCertName)),
		RevokedServerKey:  path.Join(root, fmt.Sprintf("%s-key.pem", revokedServerCertName)),
		ClientCert:        path.Join(root, fmt.Sprintf("%s-cert.pem", clientCertName)),
		ClientKey:         path.Join(root, fmt.Sprintf("%s-key.pem", clientCertName)),
		ClientCA:          path.Join(root, fmt.Sprintf("%s-cert.pem", clientCAName)),
		ClientCRL:         clientCRLPath,
		RevokedClientCert: path.Join(root, fmt.Sprintf("%s-cert.pem", revokedClientCertName)),
		RevokedClientKey:  path.Join(root, fmt.Sprintf("%s-key.pem", revokedClientCertName)),
		CombinedCRL:       combinedCRLPath,
		ServerName:        serverCertCommonName,
		RevokedServerName: revokedServerCertCommonName,
		RevokedClientName: revokedClientCertCommonName,
	}
}
