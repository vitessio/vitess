package endtoend

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestGetSchema(t *testing.T) {
	ctx := context.Background()

	topo := memorytopo.NewServer("zone1", "zone2", "zone3")

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  uuid.New().ID(),
		},
		Hostname: "abcd",
		Keyspace: "testkeyspace",
		Shard:    "-",
		Type:     topodatapb.TabletType_MASTER,
	}
	require.NoError(t, topo.CreateTablet(ctx, tablet))

	sd := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:       "foo",
				RowCount:   1000,
				DataLength: 1000000,
				Schema: `CREATE TABLE foo (
	id INT(11) NOT NULL,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
				Columns: []string{
					"id",
					"name",
				},
				PrimaryKeyColumns: []string{
					"id",
				},
				Fields: []*querypb.Field{
					{
						Name:         "id",
						Type:         querypb.Type_INT32,
						Table:        "foo",
						OrgTable:     "foo",
						Database:     "vt_testkeyspace",
						OrgName:      "id",
						ColumnLength: 11,
						Charset:      63,
						Decimals:     0,
					},
					{
						Name:         "name",
						Type:         querypb.Type_VARCHAR,
						Table:        "foo",
						OrgTable:     "foo",
						Database:     "vt_testkeyspace",
						OrgName:      "name",
						ColumnLength: 1020,
						Charset:      45,
						Decimals:     0,
					},
				},
			},
			{
				Name:       "bar",
				RowCount:   1,
				DataLength: 10,
				Schema: `CREATE TABLE bar (
	id INT(11) NOT NULL
	foo_id INT(11) NOT NULL
	is_active TINYINT(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB`,
				Columns: []string{
					"id",
					"foo_id",
					"is_active",
				},
				PrimaryKeyColumns: []string{
					"id",
				},
				Fields: []*querypb.Field{
					{
						Name:         "id",
						Type:         querypb.Type_INT32,
						Table:        "bar",
						OrgTable:     "bar",
						Database:     "vt_testkeyspace",
						OrgName:      "id",
						ColumnLength: 11,
						Charset:      63,
						Decimals:     0,
					},
					{
						Name:         "foo_id",
						Type:         querypb.Type_INT32,
						Table:        "bar",
						OrgTable:     "bar",
						Database:     "vt_testkeyspace",
						OrgName:      "foo_id",
						ColumnLength: 11,
						Charset:      63,
						Decimals:     0,
					},
					{
						Name:         "is_active",
						Type:         querypb.Type_INT8,
						Table:        "bar",
						OrgTable:     "bar",
						Database:     "vt_testkeyspace",
						OrgName:      "is_active",
						ColumnLength: 1,
						Charset:      63,
						Decimals:     0,
					},
				},
			},
		},
	}

	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{
			topoproto.TabletAliasString(tablet.Alias): {
				Schema: sd,
				Error:  nil,
			},
		},
	}

	tmclient.RegisterTabletManagerClientFactory(t.Name(), func() tmclient.TabletManagerClient {
		return &tmc
	})
	*tmclient.TabletManagerProtocol = t.Name()

	logger := logutil.NewMemoryLogger()

	err := vtctl.RunCommand(ctx, wrangler.New(logger, topo, &tmc), []string{
		"GetSchema",
		topoproto.TabletAliasString(tablet.Alias),
	})
	require.NoError(t, err)

	events := logger.Events
	assert.Equal(t, 1, len(events), "expected 1 event from GetSchema")
	val := events[0].Value

	actual := &tabletmanagerdatapb.SchemaDefinition{}
	err = json2.Unmarshal([]byte(val), actual)
	require.NoError(t, err)

	assert.Equal(t, sd, actual)

	// reset for the next invocation, where we verify that passing
	// -table_sizes_only does not include the create table statement or columns.
	logger.Events = nil
	sd = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "foo",
				RowCount:          1000,
				DataLength:        1000000,
				Columns:           []string{},
				PrimaryKeyColumns: []string{},
				Fields:            []*querypb.Field{},
			},
			{
				Name:              "bar",
				RowCount:          1,
				DataLength:        10,
				Columns:           []string{},
				PrimaryKeyColumns: []string{},
				Fields:            []*querypb.Field{},
			},
		},
	}

	err = vtctl.RunCommand(ctx, wrangler.New(logger, topo, &tmc), []string{
		"GetSchema",
		"-table_sizes_only",
		topoproto.TabletAliasString(tablet.Alias),
	})
	require.NoError(t, err)

	events = logger.Events
	assert.Equal(t, 1, len(events), "expected 1 event from GetSchema")
	val = events[0].Value

	actual = &tabletmanagerdatapb.SchemaDefinition{}
	err = json2.Unmarshal([]byte(val), actual)
	require.NoError(t, err)

	assert.Equal(t, sd, actual)
}
