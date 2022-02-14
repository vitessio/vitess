package vtgate

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestColumnNames(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "create table uks.t2(id bigint,phone bigint,msg varchar(100),primary key(id)) Engine=InnoDB")
	defer utils.Exec(t, conn, "drop table uks.t2")

	qr := utils.Exec(t, conn, "SELECT t1.id as t1id, t2.id as t2id, t2.phone as t2phn FROM ks.t1 cross join uks.t2 where t1.id = t2.id ORDER BY t2.phone")

	assert.Equal(t, 3, len(qr.Fields))
	assert.Equal(t, "t1id", qr.Fields[0].Name)
	assert.Equal(t, "t2id", qr.Fields[1].Name)
	assert.Equal(t, "t2phn", qr.Fields[2].Name)
}
