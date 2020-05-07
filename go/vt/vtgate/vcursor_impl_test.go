package vtgate

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/proto/vschema"

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
		expectedError: "no keyspace with name [ks2] found",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2:-80",
		qualifier:     "",
		expectedError: "no keyspace with name [ks2] found",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "",
		qualifier:     "ks2",
		expectedError: "no keyspace with name [ks2] found",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "",
		expectedError: "keyspace not specified",
	}}

	for _, tc := range tests {
		impl, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString}), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, nil)
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
	}
}

func TestSetTarget(t *testing.T) {
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
		expectedError: "Unknown database 'ks3' (errno 1049) (sqlstate 42000)",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks2@replica",
		expectedError: "cannot change to a non-master type in the middle of a transaction: REPLICA",
	}}

	for i, tc := range tests {
		t.Run(string(i)+"#"+tc.targetString, func(t *testing.T) {
			vc, _ := newVCursorImpl(context.Background(), NewSafeSession(&vtgatepb.Session{InTransaction: true}), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, nil)
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
