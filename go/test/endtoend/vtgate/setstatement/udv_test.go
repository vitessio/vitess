package setstatement

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table test(
		id bigint,
		val varchar(16),
		primary key(id)
	)Engine=InnoDB;`

	vSchema = `
		{	
			"sharded":true,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				}
			},	
			"tables": {
				"test":{
					"column_vindexes": [
						{
							"column": "id",
							"name": "hash_index"
						}
					]
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}

func TestSet(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	//Initialize user defined variables
	exec(t, conn, "set @foo = 'abc', @bar = 'xyz'")

	//Select user defined variables
	qr := exec(t, conn, "select @foo, @bar")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARCHAR("abc") VARCHAR("xyz")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Insert into test table
	qr = exec(t, conn, "insert into test(id, val) values(1, @foo), (2, @bar)")
	require.Equal(t, uint64(2), qr.RowsAffected)

	qr = exec(t, conn, "select id, val from test order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("abc")] [INT64(2) VARCHAR("xyz")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test select calls to main table and verify expected id.
	qr = exec(t, conn, "select id, val  from test where val=@foo")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("abc")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test next available seq id from cache
	qr = exec(t, conn, "delete from test where val = @bar")
	require.Equal(t, uint64(1), qr.RowsAffected)

	//Test next_id from seq table which should be the increased by cache value(id+cache)
	qr = exec(t, conn, "select id, val from test where val = @bar")
	require.Empty(t, qr.Rows)

	// Test insert with no auto-inc
	qr = exec(t, conn, "update test set val=@bar where val=@foo")
	require.Equal(t, uint64(1), qr.RowsAffected)

	//Test next_id from seq table which should be the increased by cache value(id+cache)
	qr = exec(t, conn, "select id, val from test where val=@bar")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("xyz")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "delete from test")
	require.Equal(t, uint64(1), qr.RowsAffected)
}
