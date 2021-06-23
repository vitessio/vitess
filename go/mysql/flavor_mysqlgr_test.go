package mysql

import (
	"testing"

	"gotest.tools/assert"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMysqlGRParsePrimaryGroupMember(t *testing.T) {
	res := ReplicationStatus{}
	rows := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("host1")),
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
	}
	parsePrimaryGroupMember(&res, rows)
	assert.Equal(t, "host1", res.MasterHost)
	assert.Equal(t, 10, res.MasterPort)
	assert.Equal(t, false, res.IOThreadRunning)
	assert.Equal(t, false, res.SQLThreadRunning)
}

func TestMysqlGRReplicationApplierLagParse(t *testing.T) {
	res := ReplicationStatus{}
	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("NULL")),
	}
	parseReplicationApplierLag(&res, row)
	// strconv.NumError will leave SecondsBehindMaster unset
	assert.Equal(t, uint(0), res.SecondsBehindMaster)
	row = []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("100")),
	}
	parseReplicationApplierLag(&res, row)
	assert.Equal(t, uint(100), res.SecondsBehindMaster)
}
