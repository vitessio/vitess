/*
Copyright 2017 Google Inc.

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

package grpcclient

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	// grpc doesn't return underlying errors. So, we have
	// to rely on logs to know the root cause if a request
	// fails.
	_ "google.golang.org/grpc/grpclog/glogger"

	"github.com/youtube/vitess/go/vt/grpccommon"
	"github.com/youtube/vitess/go/vt/vttls"
)

// Dial creates a grpc connection to the given target.
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	newopts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(*grpccommon.MaxMessageSize),
			grpc.MaxCallSendMsgSize(*grpccommon.MaxMessageSize),
		),
	}
	newopts = append(newopts, opts...)
	return grpc.Dial(target, newopts...)
}

// SecureDialOption returns the gRPC dial option to use for the
// given client connection. It is either using TLS, or Insecure if
// nothing is set.
func SecureDialOption(cert, key, ca, name string) (grpc.DialOption, error) {
	// No security options set, just return.
	if (cert == "" || key == "") && ca == "" {
		return grpc.WithInsecure(), nil
	}

	// Load the config.
	config, err := vttls.ClientConfig(cert, key, ca, name)
	if err != nil {
		return nil, err
	}

	// Create the creds server options.
	creds := credentials.NewTLS(config)
	return grpc.WithTransportCredentials(creds), nil
}
