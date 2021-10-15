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

// Generate mysqldata.go from the JSON information dumped from MySQL
//go:generate go run ./tools/makemysqldata/

package collations

import (
	"fmt"
)

type Collation interface {
	init()

	Id() uint
	Name() string
	Collate(left, right []byte, isPrefix bool) int
	WeightString(dst []byte, numCodepoints int, src []byte, pad bool) []byte
	WeightStringLen(numBytes int) int
}

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var collationsByName = make(map[string]Collation)
var collationsById = make(map[uint]Collation)

func register(c Collation) {
	duplicatedCharset := func(old Collation) {
		panic(fmt.Sprintf("duplicated collation: %s[%d] (existing collation is %s[%d])",
			c.Name(), c.Id(), old.Name(), old.Id(),
		))
	}
	if old, found := collationsByName[c.Name()]; found {
		duplicatedCharset(old)
	}
	if old, found := collationsById[c.Id()]; found {
		duplicatedCharset(old)
	}
	collationsByName[c.Name()] = c
	collationsById[c.Id()] = c
}

func LookupByName(name string) Collation {
	csi := collationsByName[name]
	if csi != nil {
		csi.init()
	}
	return csi
}

func LookupById(id uint) Collation {
	csi := collationsById[id]
	if csi != nil {
		csi.init()
	}
	return csi
}

func All() (all []Collation) {
	all = make([]Collation, 0, len(collationsById))
	for _, col := range collationsById {
		col.init()
		all = append(all, col)
	}
	return
}
