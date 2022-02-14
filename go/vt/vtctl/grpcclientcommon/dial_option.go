// Package grpcclientcommon defines the flags shared by both grpcvtctlclient and
// grpcvtctldclient.
package grpcclientcommon

import (
	"flag"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
)

var (
	cert = flag.String("vtctld_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("vtctld_grpc_key", "", "the key to use to connect")
	ca   = flag.String("vtctld_grpc_ca", "", "the server ca to use to validate servers when connecting")
	crl  = flag.String("vtctld_grpc_crl", "", "the server crl to use to validate server certificates when connecting")
	name = flag.String("vtctld_grpc_server_name", "", "the server name to use to validate server certificate")
)

// SecureDialOption returns a grpc.DialOption configured to use TLS (or
// insecure if no flags were set) based on the vtctld_grpc_* flags declared by
// this package.
func SecureDialOption() (grpc.DialOption, error) {
	return grpcclient.SecureDialOption(*cert, *key, *ca, *crl, *name)
}
