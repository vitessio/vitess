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
// +build gofuzz

package vstreamer

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Fuzz implements the fuzzer
func Fuzz(data []byte) int {
	var kspb vschemapb.Keyspace
	c := fuzz.NewConsumer(data)
	err := c.GenerateStruct(&kspb)
	if err != nil {
		return -1
	}
	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks": &kspb,
		},
	}
	vschema, err := vindexes.BuildVSchema(srvVSchema)
	if err != nil {
		return -1
	}

	// Create a fuzzed Table
	t1 := &Table{}
	err = c.GenerateStruct(t1)
	if err != nil {
		return -1
	}

	testLocalVSchema := &localVSchema{
		keyspace: "ks",
		vschema:  vschema,
	}

	str1, err := c.GetString()
	if err != nil {
		return -1
	}
	str2, err := c.GetString()
	if err != nil {
		return -1
	}
	_, _ = buildPlan(t1, testLocalVSchema, &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{
			{Match: str1, Filter: str2},
		},
	})
	return 1
}
