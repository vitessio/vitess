package binlogplayer

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

type fakeDBClient struct {
}

// NewFakeDBClient returns a fake DBClient. Its functions return
// preset responses to requests.
func NewFakeDBClient() DBClient {
	return &fakeDBClient{}
}

func (dc *fakeDBClient) DBName() string {
	return "db"
}

func (dc *fakeDBClient) Connect() error {
	return nil
}

func (dc *fakeDBClient) Begin() error {
	return nil
}

func (dc *fakeDBClient) Commit() error {
	return nil
}

func (dc *fakeDBClient) Rollback() error {
	return nil
}

func (dc *fakeDBClient) Close() {
}

func (dc *fakeDBClient) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
	query = strings.ToLower(query)
	switch {
	case strings.HasPrefix(query, "insert"):
		return &sqltypes.Result{InsertID: 1}, nil
	case strings.HasPrefix(query, "update"):
		return &sqltypes.Result{RowsAffected: 1}, nil
	case strings.HasPrefix(query, "delete"):
		return &sqltypes.Result{RowsAffected: 1}, nil
	case strings.HasPrefix(query, "select"):
		if strings.Contains(query, "where") {
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|state|source|message",
					"int64|varchar|varchar|varchar",
				),
				`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > |`,
			), nil
		}
		return &sqltypes.Result{}, nil
	case strings.HasPrefix(query, "use"):
		return &sqltypes.Result{}, nil
	}
	return nil, fmt.Errorf("unexpected: %v", query)
}
