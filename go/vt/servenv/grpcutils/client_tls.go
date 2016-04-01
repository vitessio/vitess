package grpcutils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ClientSecureDialOption returns the gRPC dial option to use for the given client
// connection. It is either using TLS, or Insecure if nothing is set.
func ClientSecureDialOption(cert, key, ca, name string) (grpc.DialOption, error) {
	// no secuirty options set, just return
	if (cert == "" || key == "") && ca == "" {
		return grpc.WithInsecure(), nil
	}

	config := &tls.Config{}

	// load the client-side cert & key if any
	if cert != "" && key != "" {
		crt, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("failed to load cert/key: %v", err)
		}
		config.Certificates = []tls.Certificate{crt}
	}

	// load the server ca if any
	if ca != "" {
		b, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca file: %v", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("failed to append certificates")
		}
		config.RootCAs = cp
	}

	// set the server name if any
	if name != "" {
		config.ServerName = name
	}

	// create the creds server options
	creds := credentials.NewTLS(config)
	return grpc.WithTransportCredentials(creds), nil
}
