package clustertest

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestVtgateProcess(t *testing.T) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: ClusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	defer conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	exec(t, conn, "insert into customer(id, email) values(1,'email1')")

	qr := exec(t, conn, "select id, email from customer")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("email1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
