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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type noopProvisioner struct{}

var ErrNoopInUse = vterrors.Errorf(
	vtrpcpb.Code_UNIMPLEMENTED,
	"noop provisioner in use. select a different provisioner using vtgate flag -provisioner_type",
)

func (noopProvisioner) RequestCreateKeyspace(ctx context.Context, keyspace string) error {
	return ErrNoopInUse
}

func (noopProvisioner) RequestDeleteKeyspace(ctx context.Context, keyspace string) error {
	return ErrNoopInUse
}

func init() {
	provisioners["noop"] = noopProvisioner{}
}

