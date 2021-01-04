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
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	provisioners = make(map[string]Provisioner)
)

func factory(provisionerType string) Provisioner {
	p, ok := provisioners[provisionerType]
	if !ok {
		log.Error(vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"failed to find %s provisioner, defaulting to noop",
			provisionerType,
			))
		return noopProvisioner{}
	}
	return p
}
