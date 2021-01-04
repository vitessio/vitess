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

// Package trace contains a helper interface that allows various tracing
// tools to be plugged in to components using this interface. If no plugin is
// registered, the default one makes all trace calls into no-ops.
package provision

import (
	"context"
	"flag"
	"time"
)

var (
	//FIXME: _ or -, docstrings
	provisionerType = flag.String(
		"provision_type",
		"noop",
		"Specifies which provision implementation to use. Available options are: noop, grpc.",
	)
	ProvisionerTimeout = flag.Duration(
		"provision_timeout",
		time.Duration(5 * time.Minute),
		"Database DDL statements are synchronous from the perspective of a connected user. This specifies" +
			"the maximum time to wait before returning an error. Note that this the ONLY has an effect on the user" +
			" experience. The actual provision operation runs asynchronously for an unbounded amount of time.",
	)
)

/*
The contract for the methods of Provisioner is that they return nil if they have successfully received your request.
The caller still needs to check with topo to see if the keyspace has been created or deleted.
The caller does not need to handle retries.
 */
type Provisioner interface {
	RequestCreateKeyspace(ctx context.Context, keyspace string) error
	RequestDeleteKeyspace(ctx context.Context, keyspace string) error
}

func RequestCreateKeyspace(ctx context.Context, keyspace string) error {
	return factory(*provisionerType).RequestCreateKeyspace(ctx, keyspace)
}

func RequestDeleteKeyspace(ctx context.Context, keyspace string) error {
	return factory(*provisionerType).RequestDeleteKeyspace(ctx, keyspace)
}
