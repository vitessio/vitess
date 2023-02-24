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
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const (
	// CA is the name of the CA toplevel cert.
	CA          = "ca"
	permissions = 0700
)

func loadCert(certPath string) (*x509.Certificate, error) {
	certData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(certData)
	if block == nil {
		return nil, errors.New("failed to parse certificate PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

func saveCert(certificate *x509.Certificate, certPath string) error {
	out := &bytes.Buffer{}
	err := pem.Encode(out, &pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	if err != nil {
		return err
	}
	return os.WriteFile(certPath, out.Bytes(), permissions)
}

func generateKey() (crypto.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func loadKey(keyPath string) (crypto.PrivateKey, error) {
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(keyData)
	if block == nil {
		return nil, errors.New("failed to parse key PEM")
	}

	switch block.Type {
	case "PRIVATE KEY":
		return x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(block.Bytes)
	default:
		return nil, fmt.Errorf("unknown private key format: %+v", block.Type)
	}
}

func saveKey(key crypto.PrivateKey, keyPath string) error {
	keyData, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	out := &bytes.Buffer{}
	err = pem.Encode(out, &pem.Block{Type: "PRIVATE KEY", Bytes: keyData})
	if err != nil {
		return err
	}
	return os.WriteFile(keyPath, out.Bytes(), permissions)
}

// pubkey is an interface to get a public key from a private key
// The Go specification for a private key defines that this always
// exists, although there's no interface for it since it would break
// backwards compatibility. See https://pkg.go.dev/crypto#PrivateKey
type pubKey interface {
	Public() crypto.PublicKey
}

func publicKey(priv crypto.PrivateKey) crypto.PublicKey {
	return priv.(pubKey).Public()
}

func signCert(parent *x509.Certificate, parentPriv crypto.PrivateKey, certPub crypto.PublicKey, commonName string, serial int64, ca bool) (*x509.Certificate, error) {
	keyUsage := x509.KeyUsageDigitalSignature
	var extKeyUsage []x509.ExtKeyUsage
	var dnsNames []string
	var ipAddresses []net.IP

	if ca {
		keyUsage = keyUsage | x509.KeyUsageCRLSign | x509.KeyUsageCertSign
	} else {
		extKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		dnsNames = []string{"localhost", commonName}
		ipAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now().Add(-30 * time.Second),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              keyUsage,
		ExtKeyUsage:           extKeyUsage,
		BasicConstraintsValid: true,
		IsCA:                  ca,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddresses,
	}

	// No parent defined means we create a self signed one.
	if parent == nil {
		parent = &template
	}

	certificate, err := x509.CreateCertificate(rand.Reader, &template, parent, certPub, parentPriv)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certificate)
}

// CreateCA creates the toplevel 'ca' certificate and key, and places it
// in the provided directory. Temporary files are also created in that
// directory.
func CreateCA(root string) {
	log.Infof("Creating test root CA in %v", root)
	keyPath := path.Join(root, "ca-key.pem")
	certPath := path.Join(root, "ca-cert.pem")

	priv, err := generateKey()
	if err != nil {
		log.Fatal(err)
	}

	err = saveKey(priv, keyPath)
	if err != nil {
		log.Fatal(err)
	}

	ca, err := signCert(nil, priv, publicKey(priv), CA, 1, true)
	if err != nil {
		log.Fatal(err)
	}

	err = saveCert(ca, certPath)
	if err != nil {
		log.Fatal(err)
	}
}

func CreateIntermediateCA(root, parent, serial, name, commonName string) {
	caKeyPath := path.Join(root, parent+"-key.pem")
	caCertPath := path.Join(root, parent+"-cert.pem")
	keyPath := path.Join(root, name+"-key.pem")
	certPath := path.Join(root, name+"-cert.pem")

	caKey, err := loadKey(caKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	caCert, err := loadCert(caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	priv, err := generateKey()
	if err != nil {
		log.Fatal(err)
	}

	err = saveKey(priv, keyPath)
	if err != nil {
		log.Fatal(err)
	}

	serialNr, err := strconv.ParseInt(serial, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	intermediate, err := signCert(caCert, caKey, publicKey(priv), commonName, serialNr, true)
	if err != nil {
		log.Fatal(err)
	}
	err = saveCert(intermediate, certPath)
	if err != nil {
		log.Fatal(err)
	}
}

// CreateSignedCert creates a new certificate signed by the provided parent,
// with the provided serial number, name and common name.
// name is the file name to use. Common Name is the certificate common name.
func CreateSignedCert(root, parent, serial, name, commonName string) {
	log.Infof("Creating signed cert and key %v", commonName)

	caKeyPath := path.Join(root, parent+"-key.pem")
	caCertPath := path.Join(root, parent+"-cert.pem")
	keyPath := path.Join(root, name+"-key.pem")
	certPath := path.Join(root, name+"-cert.pem")

	caKey, err := loadKey(caKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	caCert, err := loadCert(caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	priv, err := generateKey()
	if err != nil {
		log.Fatal(err)
	}

	err = saveKey(priv, keyPath)
	if err != nil {
		log.Fatal(err)
	}

	serialNr, err := strconv.ParseInt(serial, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	leaf, err := signCert(caCert, caKey, publicKey(priv), commonName, serialNr, false)
	if err != nil {
		log.Fatal(err)
	}

	err = saveCert(leaf, certPath)
	if err != nil {
		log.Fatal(err)
	}
}

// CreateCRL creates a new empty certificate revocation list
// for the provided parent
func CreateCRL(root, parent string) {
	log.Infof("Creating CRL for root CA in %v", root)
	caKeyPath := path.Join(root, parent+"-key.pem")
	caCertPath := path.Join(root, parent+"-cert.pem")
	crlPath := path.Join(root, parent+"-crl.pem")

	caKey, err := loadKey(caKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	caCert, err := loadCert(caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	crlList, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		RevokedCertificates: nil,
		Number:              big.NewInt(1),
	}, caCert, caKey.(crypto.Signer))
	if err != nil {
		log.Fatal(err)
	}

	out := &bytes.Buffer{}
	err = pem.Encode(out, &pem.Block{Type: "X509 CRL", Bytes: crlList})
	if err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile(crlPath, out.Bytes(), permissions)
	if err != nil {
		log.Fatal(err)
	}
}

// RevokeCertAndRegenerateCRL revokes a provided certificate under the
// provided parent CA and regenerates the CRL file for that parent
func RevokeCertAndRegenerateCRL(root, parent, name string) {
	log.Infof("Revoking certificate %s", name)
	caKeyPath := path.Join(root, parent+"-key.pem")
	caCertPath := path.Join(root, parent+"-cert.pem")
	crlPath := path.Join(root, parent+"-crl.pem")
	certPath := path.Join(root, name+"-cert.pem")

	certificate, err := loadCert(certPath)
	if err != nil {
		log.Fatal(err)
	}

	// Check if CRL already exists. If it doesn't,
	// create an empty CRL to start with.
	_, err = os.Stat(crlPath)
	if errors.Is(err, os.ErrNotExist) {
		CreateCRL(root, parent)
	}

	data, err := os.ReadFile(crlPath)
	if err != nil {
		log.Fatal(err)
	}
	crlList, err := x509.ParseCRL(data) //nolint:staticcheck
	if err != nil {
		log.Fatal(err)
	}

	revoked := crlList.TBSCertList.RevokedCertificates
	revoked = append(revoked, pkix.RevokedCertificate{
		SerialNumber:   certificate.SerialNumber,
		RevocationTime: time.Now(),
	})

	caKey, err := loadKey(caKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	caCert, err := loadCert(caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	newCrl, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		RevokedCertificates: revoked,
		Number:              big.NewInt(int64(crlList.TBSCertList.Version) + 1),
	}, caCert, caKey.(crypto.Signer))
	if err != nil {
		log.Fatal(err)
	}

	out := &bytes.Buffer{}
	err = pem.Encode(out, &pem.Block{Type: "X509 CRL", Bytes: newCrl})
	if err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile(crlPath, out.Bytes(), permissions)
	if err != nil {
		log.Fatal(err)
	}
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

	CreateIntermediateCA(root, CA, serverCASerial, serverCAName, serverCACommonName)
	CreateSignedCert(root, serverCAName, serverSerial, serverCertName, serverCertCommonName)
	CreateSignedCert(root, serverCAName, revokedServerSerial, revokedServerCertName, revokedServerCertCommonName)
	RevokeCertAndRegenerateCRL(root, serverCAName, revokedServerCertName)

	CreateIntermediateCA(root, CA, clientCASerial, clientCAName, clientCACommonName)
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

	err = os.WriteFile(combinedCRLPath, append(serverCRLBytes, clientCRLBytes...), permissions)
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
