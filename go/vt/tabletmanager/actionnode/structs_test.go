package actionnode

import (
	"encoding/json"
	"testing"

	"github.com/youtube/vitess/go/bson"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// These tests encode a slaveWasRestartedTestArgs (same as
// SlaveWasRestartedArgs but with a few more arguments) and try to
// decode it as a SlaveWasRestartedArgs, and vice versa

type slaveWasRestartedTestArgs struct {
	Parent               *topodatapb.TabletAlias
	ExpectedMasterAddr   string
	ExpectedMasterIPAddr string
}

func TestMissingFieldsJson(t *testing.T) {
	swra := &slaveWasRestartedTestArgs{
		Parent: &topodatapb.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
		ExpectedMasterAddr:   "a1",
		ExpectedMasterIPAddr: "i1",
	}
	data, err := json.MarshalIndent(swra, "", "  ")
	if err != nil {
		t.Fatalf("cannot marshal: %v", err)
	}

	output := &SlaveWasRestartedArgs{}
	if err = json.Unmarshal(data, output); err != nil {
		t.Errorf("Cannot re-decode struct without field: %v", err)
	}
}

func TestExtraFieldsJson(t *testing.T) {
	swra := &SlaveWasRestartedArgs{
		Parent: &topodatapb.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
	}
	data, err := json.MarshalIndent(swra, "", "  ")
	if err != nil {
		t.Fatalf("cannot marshal: %v", err)
	}

	output := &slaveWasRestartedTestArgs{}
	if err = json.Unmarshal(data, output); err != nil {
		t.Errorf("Cannot re-decode struct without field: %v", err)
	}
}

func TestMissingFieldsBson(t *testing.T) {
	swra := &slaveWasRestartedTestArgs{
		Parent: &topodatapb.TabletAlias{
			Uid:  1,
			Cell: "aa",
		},
		ExpectedMasterAddr:   "a1",
		ExpectedMasterIPAddr: "i1",
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
		Parent: &topodatapb.TabletAlias{
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
