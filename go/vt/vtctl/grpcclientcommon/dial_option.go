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
	"flag"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
)

var (
	cert = flag.String("vtctld_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("vtctld_grpc_key", "", "the key to use to connect")
	ca   = flag.String("vtctld_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("vtctld_grpc_server_name", "", "the server name to use to validate server certificate")
)

// SecureDialOption returns a grpc.DialOption configured to use TLS (or
// insecure if no flags were set) based on the vtctld_grpc_* flags declared by
// this package.
func SecureDialOption() (grpc.DialOption, error) {
	return grpcclient.SecureDialOption(*cert, *key, *ca, *name)
}
