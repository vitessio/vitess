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
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/pflag"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

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

// VerbosityLevelFlag implements the pflag.Value interface, for parsing a command-line value
// into a vtctldatapb.VerbosityLevel.
type VerbosityLevelFlag vtctldatapb.VerbosityLevel

// String is part of the pflag.Value interfaces.
func (vlf *VerbosityLevelFlag) String() string {
	return vtctldatapb.VerbosityLevel(*vlf).String()
}

// ParseVerbosityLevelFlag parses the string value into the enum type.
func ParseVerbosityLevelFlag(param string) (vtctldatapb.VerbosityLevel, error) {
	value, ok := vtctldatapb.VerbosityLevel_value[strings.ToUpper(param)]
	if !ok {
		return vtctldatapb.VerbosityLevel(-1), fmt.Errorf("unknown VerbosityLevel %q, valid values are %s",
			param, strings.Join(GetVerbosityLevelFlagOptions(), ", "))
	}
	return vtctldatapb.VerbosityLevel(value), nil
}

// GetVerbosityLevelFlagOptions returns a list of valid values which can be provided for
// the VerbosityLevelFlag.
func GetVerbosityLevelFlagOptions() []string {
	enumVals := maps.Values(vtctldatapb.VerbosityLevel_value)
	slices.Sort(enumVals)
	strVals := make([]string, len(enumVals))
	for i, v := range enumVals {
		strVals[i] = vtctldatapb.VerbosityLevel(v).String()
	}
	return strVals
}

// Set is part of the pflag.Value interfaces.
func (vlf *VerbosityLevelFlag) Set(v string) error {
	t, err := ParseVerbosityLevelFlag(v)
	*vlf = VerbosityLevelFlag(t)
	return err
}

// Type is part of the pflag.Value interface.
func (*VerbosityLevelFlag) Type() string { return "vtctldatapb.VerbosityLevel" }
