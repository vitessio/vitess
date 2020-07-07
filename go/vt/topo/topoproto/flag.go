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
	"flag"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletTypeListVar defines a []TabletType flag with the specified name and usage
// string. The argument 'p' points to a []TabletType in which to store the value of the flag.
func TabletTypeListVar(p *[]topodatapb.TabletType, name string, usage string) {
	flag.Var((*TabletTypeListValue)(p), name, usage)
}

// TabletTypeVar defines a TabletType flag with the specified name, default value and usage
// string. The argument 'p' points to a tabletType in which to store the value of the flag.
func TabletTypeVar(p *topodatapb.TabletType, name string, defaultValue topodatapb.TabletType, usage string) {
	*p = defaultValue
	flag.Var((*TabletTypeFlag)(p), name, usage)
}

// TabletTypeListValue implements the flag.Value interface, for parsing a command-line comma-separated
// list of value into a slice of TabletTypes.
type TabletTypeListValue []topodatapb.TabletType

// String is part of the flag.Value interface.
func (ttlv *TabletTypeListValue) String() string {
	return strings.Join(MakeStringTypeList(*ttlv), ",")
}

// Set is part of the flag.Value interface.
func (ttlv *TabletTypeListValue) Set(v string) (err error) {
	*ttlv, err = ParseTabletTypes(v)
	return err
}

// Get is for satisflying the internal flag interface.
func (ttlv *TabletTypeListValue) Get() interface{} {
	return *ttlv
}

// TabletTypeFlag implements the flag.Value interface, for parsing a command-line value into a TabletType.
type TabletTypeFlag topodatapb.TabletType

// String is part of the flag.Value interface.
func (ttf *TabletTypeFlag) String() string {
	return topodatapb.TabletType(*ttf).String()
}

// Set is part of the flag.Value interface.
func (ttf *TabletTypeFlag) Set(v string) error {
	t, err := ParseTabletType(v)
	*ttf = TabletTypeFlag(t)
	return err
}

// Get is for satisflying the internal flag interface.
func (ttf *TabletTypeFlag) Get() interface{} {
	return *ttf
}
