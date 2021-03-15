package datapump

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

type TestDataPump struct {
	t        *testing.T
	keyspace string
	dbConn   *mysql.Conn

	tables []*TestTable

	concurrency int
}

func NewTestDataPump(t *testing.T, keyspace string, dbConn *mysql.Conn, numTables int) (*TestDataPump, error) {
	tdp := &TestDataPump{
		t:           t,
		dbConn:      dbConn,
		concurrency: 1,
		keyspace:    keyspace,
	}
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("stress_test_%d", i)
		if _, err := tdp.dbConn.ExecuteFetch(fmt.Sprintf(createStatement, keyspace, tableName), 1, false); err != nil {
			return nil, err
		}

		table := NewTestTable(tableName, keyspace, dbConn)
		if table == nil {
			return nil, fmt.Errorf("could not create table %s", tableName)
		}
		tdp.tables = append(tdp.tables, table)
	}
	return tdp, nil
}

func (tdp *TestDataPump) Start(ctx context.Context) {
	for _, table := range tdp.tables {
		for i := 0; i < tdp.concurrency; i++ {
			go tdp.startPumping(ctx, table)
		}
	}
}

func (tdp *TestDataPump) startPumping(ctx context.Context, table *TestTable) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf(">>>>>>>>>>>>>>>>>>>>>> Context Done in startPumping\n")
			return
		default:
		}

		r := rand.Intn(6)
		switch r {
		case 0, 1, 2:
			require.NoError(tdp.t, table.Insert())
		case 3, 4:
			require.NoError(tdp.t, table.Update())
		case 5:
			require.NoError(tdp.t, table.Delete())
		}
	}
}
