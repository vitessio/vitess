/*
Copyright 2021 The Vitess Authors.

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

// Package grpcclientcommon defines the flags shared by both grpcvtctlclient and
// grpcvtctldclient.
package grpcclientcommon

import (
	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/servenv"
)

var cert, key, ca, crl, name string

func init() {
	servenv.OnParseFor("vtctl", RegisterFlags)
	servenv.OnParseFor("vttestserver", RegisterFlags)
	servenv.OnParseFor("vtctlclient", RegisterFlags)
	servenv.OnParseFor("vtctldclient", RegisterFlags)
}

func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&cert, "vtctld_grpc_cert", cert, "the cert to use to connect")
	fs.StringVar(&key, "vtctld_grpc_key", key, "the key to use to connect")
	fs.StringVar(&ca, "vtctld_grpc_ca", ca, "the server ca to use to validate servers when connecting")
	fs.StringVar(&crl, "vtctld_grpc_crl", crl, "the server crl to use to validate server certificates when connecting")
	fs.StringVar(&name, "vtctld_grpc_server_name", name, "the server name to use to validate server certificate")
}

// SecureDialOption returns a grpc.DialOption configured to use TLS (or
// insecure if no flags were set) based on the vtctld_grpc_* flags declared by
// this package.
func SecureDialOption() (grpc.DialOption, error) {
	return grpcclient.SecureDialOption(cert, key, ca, crl, name)
}
