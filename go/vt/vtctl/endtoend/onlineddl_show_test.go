package endtoend

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestShowOnlineDDL_All(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "all",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid like '%'  order by `id` ASC ")
}

func TestShowOnlineDDL_Recent(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "recent",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where requested_timestamp > now() - interval 1 week  order by `id` ASC ")
}

func TestShowOnlineDDL_ID(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "82fa54ac_e83e_11ea_96b7_f875a4d24e90",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid='82fa54ac_e83e_11ea_96b7_f875a4d24e90'  order by `id` ASC ")
}

func TestShowOnlineDDL_Order_Descending(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "--order", "descending", "testkeyspace", "show", "all",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid like '%'  order by `id` DESC ")
}

func TestShowOnlineDDL_Order_Ascending(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "all", "--order", "ascending",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid like '%'  order by `id` ASC ")
}

func TestShowOnlineDDL_Skip(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "--skip", "20", "--limit", "5", "testkeyspace", "show", "all",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid like '%'  order by `id` ASC LIMIT 20,5")
}

func TestShowOnlineDDL_Limit(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "--limit", "55", "testkeyspace", "show", "all",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_uuid like '%'  order by `id` ASC LIMIT 0,55")
}

func TestShowOnlineDDL_Running(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "running",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_status='running'  order by `id` ASC ")
}

func TestShowOnlineDDL_Complete(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "complete",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_status='complete'  order by `id` ASC ")
}

func TestShowOnlineDDL_Failed(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "show", "failed",
	}, "select\n\t\t\t\t*\n\t\t\t\tfrom _vt.schema_migrations where migration_status='failed'  order by `id` ASC ")
}

func TestShowOnlineDDL_Retry(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "retry", "82fa54ac_e83e_11ea_96b7_f875a4d24e90",
	}, "update _vt.schema_migrations set migration_status='retry' where migration_uuid='82fa54ac_e83e_11ea_96b7_f875a4d24e90'")
}

func TestShowOnlineDDL_Cancel(t *testing.T) {
	onlineDDLTest(t, []string{
		"OnlineDDL", "testkeyspace", "cancel", "82fa54ac_e83e_11ea_96b7_f875a4d24e90",
	}, "update _vt.schema_migrations set migration_status='cancel' where migration_uuid='82fa54ac_e83e_11ea_96b7_f875a4d24e90'")
}

func onlineDDLTest(t *testing.T, args []string, expectedQuery string) {
	t.Helper()
	ctx := context.Background()

	fakeTopo := memorytopo.NewServer("zone1", "zone2", "zone3")

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  uuid.New().ID(),
		},
		Hostname: "abcd",
		Keyspace: "testkeyspace",
		Shard:    "-",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, fakeTopo.CreateTablet(ctx, tablet))

	tmc := testutil.TabletManagerClient{}
	tmclient.RegisterTabletManagerClientFactory(t.Name(), func() tmclient.TabletManagerClient {
		return &tmc
	})
	tmclienttest.SetProtocol("go.vt.vtctl.endtoend", t.Name())

	logger := logutil.NewMemoryLogger()
	wr := wrangler.New(logger, fakeTopo, &tmc)

	wr.VExecFunc = func(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {
		assert.Equal(t, query, expectedQuery)
		return nil, errors.New("I failed in the test")
	}

	err := vtctl.RunCommand(ctx, wr, args)
	assert.ErrorContains(t, err, "I failed in the test")
}
