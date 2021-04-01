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

package cli

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// StringMapValue augments flagutil.StringMapValue so it can be used as a
// pflag.Value.
type StringMapValue struct {
	flagutil.StringMapValue
}

// Type is part of the pflag.Value interface.
func (v *StringMapValue) Type() string {
	return "cli.StringMapValue"
}

// KeyspaceIDTypeFlag adds the pflag.Value interface to a
// topodatapb.KeyspaceIdType.
type KeyspaceIDTypeFlag topodatapb.KeyspaceIdType

var _ pflag.Value = (*KeyspaceIDTypeFlag)(nil)

// Set is part of the pflag.Value interface.
func (v *KeyspaceIDTypeFlag) Set(arg string) error {
	t, err := key.ParseKeyspaceIDType(arg)
	if err != nil {
		return err
	}

	*v = KeyspaceIDTypeFlag(t)

	return nil
}

// String is part of the pflag.Value interface.
func (v *KeyspaceIDTypeFlag) String() string {
	return key.KeyspaceIDTypeString(topodatapb.KeyspaceIdType(*v))
}

// Type is part of the pflag.Value interface.
func (v *KeyspaceIDTypeFlag) Type() string {
	return "cli.KeyspaceIdTypeFlag"
}

// KeyspaceTypeFlag adds the pflag.Value interface to a topodatapb.KeyspaceType.
type KeyspaceTypeFlag topodatapb.KeyspaceType

var _ pflag.Value = (*KeyspaceTypeFlag)(nil)

// Set is part of the pflag.Value interface.
func (v *KeyspaceTypeFlag) Set(arg string) error {
	kt, err := topoproto.ParseKeyspaceType(arg)
	if err != nil {
		return err
	}

	*v = KeyspaceTypeFlag(kt)

	return nil
}

// String is part of the pflag.Value interface.
func (v *KeyspaceTypeFlag) String() string {
	return topoproto.KeyspaceTypeString(topodatapb.KeyspaceType(*v))
}

// Type is part of the pflag.Value interface.
func (v *KeyspaceTypeFlag) Type() string {
	return "cli.KeyspaceTypeFlag"
}
