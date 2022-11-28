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

package topoproto

import (
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletTypeListFlag implements the pflag.Value interface, for parsing a command-line comma-separated
// list of values into a slice of TabletTypes.
type TabletTypeListFlag []topodatapb.TabletType

// String is part of the pflag.Value interface.
func (ttlv *TabletTypeListFlag) String() string {
	return strings.Join(MakeStringTypeList(*ttlv), ",")
}

// Set is part of the pflag.Value interface.
func (ttlv *TabletTypeListFlag) Set(v string) (err error) {
	*ttlv, err = ParseTabletTypes(v)
	return err
}

// Type is part of the pflag.Value interface.
func (ttlv *TabletTypeListFlag) Type() string {
	return "strings"
}

// TabletTypeFlag implements the pflag.Value interface, for parsing a command-line value into a TabletType.
type TabletTypeFlag topodatapb.TabletType

// String is part of the pflag.Value interfaces.
func (ttf *TabletTypeFlag) String() string {
	return topodatapb.TabletType(*ttf).String()
}

// Set is part of the pflag.Value interfaces.
func (ttf *TabletTypeFlag) Set(v string) error {
	t, err := ParseTabletType(v)
	*ttf = TabletTypeFlag(t)
	return err
}

// Type is part of the pflag.Value interface.
func (*TabletTypeFlag) Type() string { return "topodatapb.TabletType" }
