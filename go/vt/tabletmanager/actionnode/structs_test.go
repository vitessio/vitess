package actionnode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/henryanand/vitess/go/bson"
	"github.com/henryanand/vitess/go/jscfg"
	"github.com/henryanand/vitess/go/vt/topo"
)

// These tests encode a slaveWasRestartedTestArgs (same as
// SlaveWasRestartedArgs but with a few more arguments) and try to
// decode it as a SlaveWasRestartedArgs, and vice versa

type slaveWasRestartedTestArgs struct {
	Parent               topo.TabletAlias
	ExpectedMasterAddr   string
	ExpectedMasterIpAddr string
	ScrapStragglers      bool
}

func TestMissingFieldsJson(t *testing.T) {
	swra := &slaveWasRestartedTestArgs{
		Parent: topo.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
		ExpectedMasterAddr:   "a1",
		ExpectedMasterIpAddr: "i1",
		ScrapStragglers:      true,
	}
	data := jscfg.ToJson(swra)

	output := &SlaveWasRestartedArgs{}
	decoder := json.NewDecoder(strings.NewReader(data))
	err := decoder.Decode(output)
	if err != nil {
		t.Errorf("Cannot re-decode struct without field: %v", err)
	}
}

func TestExtraFieldsJson(t *testing.T) {
	swra := &SlaveWasRestartedArgs{
		Parent: topo.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
	}
	data := jscfg.ToJson(swra)

	output := &slaveWasRestartedTestArgs{}
	decoder := json.NewDecoder(strings.NewReader(data))
	err := decoder.Decode(output)
	if err != nil {
		t.Errorf("Cannot re-decode struct without field: %v", err)
	}
}

func TestMissingFieldsBson(t *testing.T) {
	swra := &slaveWasRestartedTestArgs{
		Parent: topo.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
		ExpectedMasterAddr:   "a1",
		ExpectedMasterIpAddr: "i1",
		ScrapStragglers:      true,
	}
	data, err := bson.Marshal(swra)
	if err != nil {
		t.Fatal(err)
	}

	output := &SlaveWasRestartedArgs{}
	err = bson.Unmarshal(data, output)
	if err != nil {
		t.Error(err)
	}
}

func TestExtraFieldsBson(t *testing.T) {
	swra := &SlaveWasRestartedArgs{
		Parent: topo.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
	}
	data, err := bson.Marshal(swra)
	if err != nil {
		t.Fatal(err)
	}

	output := &slaveWasRestartedTestArgs{}

	err = bson.Unmarshal(data, output)
	if err != nil {
		t.Error(err)
	}
}
