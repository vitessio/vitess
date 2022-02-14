//go:build gofuzz
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
	vschema := vindexes.BuildVSchema(srvVSchema)
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
