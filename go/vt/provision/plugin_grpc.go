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

package provision

import (
	"context"
	"flag"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"time"
	"vitess.io/vitess/go/vt/proto/provision"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)


var (
	ErrNeedGrpcEndpoint = vterrors.Errorf(
		vtrpcpb.Code_FAILED_PRECONDITION,
		"need grpc endpoint to use grpc provisioning",
	)

	provisionGrpcEndpoint = flag.String(
		"provision_grpc_endpoint",
		"",
		"gRPC endpoint to connect to. This is required if `grpc` is specified for `-provision_type`.",
		)
	provisionGrpcDialTimeout = flag.Duration(
		"provision_grpc_dial_timeout",
		time.Duration(5 * time.Second),
		"Maximum time to try connecting to the gRPC endpoint before timing out.",
	)
	provisionGrpcRequestTimeout = flag.Duration(
		"provision_grpc_per_retry_timeout",
		time.Duration(5 * time.Second),
		"Maximum time to wait for a provision request to before timing out.",
	)
	provisionGrpcMaxRetries = flag.Uint(
		"provision_grpc_max_retries",
		3,
		"Maximum times to try sending a provision request.",
	)


)
type grpcProvisioner struct {}

func withOpenClient(ctx context.Context, callback func (client provision.ProvisionClient) error) error {
	if *provisionGrpcEndpoint == "" {
		return ErrNeedGrpcEndpoint
	}
	dialTimeout, cancel := context.WithTimeout(ctx, *provisionGrpcDialTimeout)
	defer cancel()

	//FIXME: tls
	conn, err := grpc.DialContext(dialTimeout, *provisionGrpcEndpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		vterrors.Wrapf(err, "dialing to grpc provisioner timed out")
	}
	defer conn.Close()

	return callback(provision.NewProvisionClient(conn))
}


func (p grpcProvisioner) RequestCreateKeyspace(ctx context.Context, keyspace string) error {
	return withOpenClient(ctx, func(client provision.ProvisionClient) error {
		req := &provision.RequestCreateKeyspaceRequest{
			Keyspace:             keyspace,
		}

		_, err := client.RequestCreateKeyspace(
			ctx,
			req,
			grpc_retry.WithPerRetryTimeout(*provisionGrpcRequestTimeout),
			grpc_retry.WithMax(*provisionGrpcMaxRetries),
			grpc_retry.WithBackoff(
				grpc_retry.BackoffLinear(1 * time.Second),
			),
		)

		return err
	})
}

func (p grpcProvisioner) RequestDeleteKeyspace(ctx context.Context, keyspace string) error {
	return withOpenClient(ctx, func(client provision.ProvisionClient) error {
		req := &provision.RequestDeleteKeyspaceRequest{
			Keyspace:             keyspace,
		}

		_, err := client.RequestDeleteKeyspace(
			ctx,
			req,
			grpc_retry.WithPerRetryTimeout(*provisionGrpcRequestTimeout),
			grpc_retry.WithMax(*provisionGrpcMaxRetries),
			grpc_retry.WithBackoff(
				grpc_retry.BackoffLinear(1 * time.Second),
			),
		)

		return err
	})
}

func init() {
	provisioners["grpc"] = grpcProvisioner{}
}