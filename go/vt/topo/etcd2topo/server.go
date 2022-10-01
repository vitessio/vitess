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

/*
Package etcd2topo implements topo.Server with etcd as the backend.

We expect the following behavior from the etcd client library:

  - Get and Delete return ErrorCodeKeyNotFound if the node doesn't exist.
  - Create returns ErrorCodeNodeExist if the node already exists.
  - Intermediate directories are always created automatically if necessary.
  - Set returns ErrorCodeKeyNotFound if the node doesn't exist already.
  - It returns ErrorCodeTestFailed if the provided version index doesn't match.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the etcd client library.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
*/
package etcd2topo

import (
	"crypto/tls"
	"crypto/x509"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/tlsutil"
	"google.golang.org/grpc"

	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
)

var (
	clientCertPath string
	clientKeyPath  string
	serverCaPath   string
)

// Factory is the consul topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root)
}

// Server is the implementation of topo.Server for etcd.
type Server struct {
	// cli is the v3 client.
	cli *clientv3.Client

	// root is the root path for this client.
	root string

	running chan struct{}
}

func init() {
	for _, cmd := range topo.FlagBinaries {
		servenv.OnParseFor(cmd, registerEtcd2TopoFlags)
	}
	topo.RegisterFactory("etcd2", Factory{})
}

func registerEtcd2TopoFlags(fs *pflag.FlagSet) {
	fs.StringVar(&clientCertPath, "topo_etcd_tls_cert", clientCertPath, "path to the client cert to use to connect to the etcd topo server, requires topo_etcd_tls_key, enables TLS")
	fs.StringVar(&clientKeyPath, "topo_etcd_tls_key", clientKeyPath, "path to the client key to use to connect to the etcd topo server, enables TLS")
	fs.StringVar(&serverCaPath, "topo_etcd_tls_ca", serverCaPath, "path to the ca to use to validate the server cert when connecting to the etcd topo server")
}

// Close implements topo.Server.Close.
// It will nil out the global and cells fields, so any attempt to
// re-use this server will panic.
func (s *Server) Close() {
	close(s.running)
	s.cli.Close()
	s.cli = nil
}

func newTLSConfig(certPath, keyPath, caPath string) (*tls.Config, error) {
	var tlscfg *tls.Config
	// If TLS is enabled, attach TLS config info.
	if certPath != "" && keyPath != "" {
		var (
			cert *tls.Certificate
			cp   *x509.CertPool
			err  error
		)

		cert, err = tlsutil.NewCert(certPath, keyPath, nil)
		if err != nil {
			return nil, err
		}

		if caPath != "" {
			cp, err = tlsutil.NewCertPool([]string{caPath})
			if err != nil {
				return nil, err
			}
		}

		tlscfg = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			RootCAs:            cp,
			InsecureSkipVerify: false,
		}
		if cert != nil {
			tlscfg.Certificates = []tls.Certificate{*cert}
		}
	}
	return tlscfg, nil
}

// NewServerWithOpts creates a new server with the provided TLS options
func NewServerWithOpts(serverAddr, root, certPath, keyPath, caPath string) (*Server, error) {
	// TODO: Rename this to NewServer and change NewServer to a name that signifies it uses the process-wide TLS settings.
	config := clientv3.Config{
		Endpoints:   strings.Split(serverAddr, ","),
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	}

	tlscfg, err := newTLSConfig(certPath, keyPath, caPath)
	if err != nil {
		return nil, err
	}

	config.TLS = tlscfg

	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &Server{
		cli:     cli,
		root:    root,
		running: make(chan struct{}),
	}, nil
}

// NewServer returns a new etcdtopo.Server.
func NewServer(serverAddr, root string) (*Server, error) {
	// TODO: Rename this to a name to signifies this function uses the process-wide TLS settings.

	return NewServerWithOpts(serverAddr, root, clientCertPath, clientKeyPath, serverCaPath)
}
