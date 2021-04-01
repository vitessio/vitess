package vtgate

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"vitess.io/vitess/go/vt/proto/vschema"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ VSchemaOperator = (*fakeVSchemaOperator)(nil)

type fakeVSchemaOperator struct {
	vschema *vindexes.VSchema
}

func (f fakeVSchemaOperator) GetCurrentSrvVschema() *vschema.SrvVSchema {
	panic("implement me")
}

func (f fakeVSchemaOperator) GetCurrentVschema() (*vindexes.VSchema, error) {
	return f.vschema, nil
}

func (f fakeVSchemaOperator) UpdateVSchema(ctx context.Context, ksName string, vschema *vschema.SrvVSchema) error {
	panic("implement me")
}

type fakeTopoServer struct {
}

// GetTopoServer returns the full topo.Server instance.
func (f *fakeTopoServer) GetTopoServer() (*topo.Server, error) {
	return nil, nil
}

// GetSrvKeyspaceNames returns the list of keyspaces served in
// the provided cell.
func (f *fakeTopoServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return []string{"ks1"}, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (f *fakeTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	zeroHexBytes, _ := hex.DecodeString("")
	eightyHexBytes, _ := hex.DecodeString("80")
	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{Name: "-80", KeyRange: &topodatapb.KeyRange{Start: zeroHexBytes, End: eightyHexBytes}},
					{Name: "80-", KeyRange: &topodatapb.KeyRange{Start: eightyHexBytes, End: zeroHexBytes}},
				},
			},
		},
	}
	return ks, nil
}

// WatchSrvVSchema starts watching the SrvVSchema object for
// the provided cell.  It will call the callback when
// a new value or an error occurs.
func (f *fakeTopoServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {

}

func TestDestinationKeyspace(t *testing.T) {
	ks1 := &vindexes.Keyspace{
		Name:    "ks1",
		Sharded: false,
	}
	ks1Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks1,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	ks2 := &vindexes.Keyspace{
		Name:    "ks2",
		Sharded: false,
	}
	ks2Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks2,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
			ks2.Name: ks2Schema,
		}}

	vschemaWith1KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
		}}

	type testCase struct {
		vschema                 *vindexes.VSchema
		targetString, qualifier string
		expectedError           string
		expectedKeyspace        string
		expectedDest            key.Destination
		expectedTabletType      topodatapb.TabletType
	}

	tests := []testCase{{
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_MASTER,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_MASTER,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_MASTER,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "ks1",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_MASTER,
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2",
		qualifier:     "",
		expectedError: "Unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2:-80",
		qualifier:     "",
		expectedError: "Unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "",
		qualifier:     "ks2",
		expectedError: "Unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "",
		expectedError: errNoKeyspace.Error(),
	}}

	for i, tc := range tests {
		t.Run(strconv.Itoa(i)+tc.targetString, func(t *testing.T) {
			impl, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString}), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false)
			impl.vschema = tc.vschema
			dest, keyspace, tabletType, err := impl.TargetDestination(tc.qualifier)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDest, dest)
				require.Equal(t, tc.expectedKeyspace, keyspace.Name)
				require.Equal(t, tc.expectedTabletType, tabletType)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

var ks1 = &vindexes.Keyspace{Name: "ks1"}
var ks1Schema = &vindexes.KeyspaceSchema{Keyspace: ks1}
var ks2 = &vindexes.Keyspace{Name: "ks2"}
var ks2Schema = &vindexes.KeyspaceSchema{Keyspace: ks2}
var vschemaWith1KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
	},
}
var vschemaWith2KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
		ks2.Name: ks2Schema,
	}}

func TestSetTarget(t *testing.T) {
	type testCase struct {
		vschema       *vindexes.VSchema
		targetString  string
		expectedError string
	}

	tests := []testCase{{
		vschema:      vschemaWith2KS,
		targetString: "",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks1",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks2",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks3",
		expectedError: "Unknown database 'ks3'",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks2@replica",
		expectedError: "Can't execute the given command because you have an active transaction",
	}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			vc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{InTransaction: true}), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false)
			vc.vschema = tc.vschema
			err := vc.SetTarget(tc.targetString)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, vc.safeSession.TargetString, tc.targetString)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestPlanPrefixKey(t *testing.T) {
	type testCase struct {
		vschema               *vindexes.VSchema
		targetString          string
		expectedPlanPrefixKey string
	}

	tests := []testCase{{
		vschema:               vschemaWith1KS,
		targetString:          "",
		expectedPlanPrefixKey: "ks1@master",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1@replica",
		expectedPlanPrefixKey: "ks1@replica",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1:-80",
		expectedPlanPrefixKey: "ks1@masterDestinationShard(-80)",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1[deadbeef]",
		expectedPlanPrefixKey: "ks1@masterKsIDsResolved(80-)",
	}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			ss := NewSafeSession(&vtgatepb.Session{InTransaction: false})
			ss.SetTargetString(tc.targetString)
			vc, err := newVCursorImpl(context.Background(), ss, sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false)
			require.NoError(t, err)
			vc.vschema = tc.vschema
			require.Equal(t, tc.expectedPlanPrefixKey, vc.planPrefixKey())
		})
	}
}

func TestFirstSortedKeyspace(t *testing.T) {
	ks1Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "xks1"}}
	ks2Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks2"}}
	ks3Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks1"}}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1Schema.Keyspace.Name: ks1Schema,
			ks2Schema.Keyspace.Name: ks2Schema,
			ks3Schema.Keyspace.Name: ks3Schema,
		}}

	vc, err := newVCursorImpl(context.Background(), NewSafeSession(nil), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: vschemaWith2KS}, vschemaWith2KS, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false)
	require.NoError(t, err)
	ks, err := vc.FirstSortedKeyspace()
	require.NoError(t, err)
	require.Equal(t, ks3Schema.Keyspace, ks)
}
