package collations

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

// RemoteCollation is a generic implementation of the Collation interface
// that supports any collation in MySQL by performing the collation
// operation directly on a remote `mysqld` instance. It is not particularly
// efficient compared to the native Collation implementations in Vitess,
// but it offers authoritative results for all collation types and can be
// used as a fallback or as a way to test our native implementations.
type RemoteCollation struct {
	name string
	id   ID

	prefix string
	suffix string

	mu   sync.Mutex
	conn *mysql.Conn
	sql  bytes2.Buffer
	hex  io.Writer
	err  error
}

func makeRemoteCollation(conn *mysql.Conn, collid ID, collname string) *RemoteCollation {
	coll := &RemoteCollation{
		name: collname,
		id:   collid,
		conn: conn,
	}

	charset := collname
	if idx := strings.IndexByte(collname, '_'); idx >= 0 {
		charset = collname[:idx]
	}

	coll.prefix = fmt.Sprintf("_%s X'", charset)
	coll.suffix = fmt.Sprintf("' COLLATE %q", collname)
	coll.hex = hex.NewEncoder(&coll.sql)
	return coll
}

func RemoteByName(conn *mysql.Conn, collname string) *RemoteCollation {
	var collid ID
	if known, ok := collationsByName[collname]; ok {
		collid = known.Id()
	}
	return makeRemoteCollation(conn, collid, collname)
}

func (c *RemoteCollation) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *RemoteCollation) init() {}

func (c *RemoteCollation) Id() ID {
	return c.id
}

func (c *RemoteCollation) Name() string {
	return c.name
}

func (c *RemoteCollation) Collate(left, right []byte, isPrefix bool) int {
	if isPrefix {
		panic("unsupported: isPrefix with remote.RemoteCollation")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sql.Reset()
	c.sql.WriteString("SELECT STRCMP(")
	c.sql.WriteString(c.prefix)
	c.hex.Write(left)
	c.sql.WriteString(c.suffix)
	c.sql.WriteString(", ")
	c.sql.WriteString(c.prefix)
	c.hex.Write(right)
	c.sql.WriteString(c.suffix)
	c.sql.WriteString(")")

	result := c.performRemoteQuery()
	if result == nil {
		return 0
	}

	var cmp int64
	cmp, c.err = result[0].ToInt64()
	return int(cmp)
}

func (c *RemoteCollation) performRemoteQuery() []sqltypes.Value {
	res, err := c.conn.ExecuteFetch(c.sql.StringUnsafe(), 1, false)
	if err != nil {
		c.err = err
		return nil
	}
	if len(res.Rows) != 1 {
		c.err = fmt.Errorf("unexpected result from MySQL: %d rows returned", len(res.Rows))
		return nil
	}
	c.err = nil
	return res.Rows[0]
}

func (c *RemoteCollation) WeightString(dst, src []byte, numCodepoints int) []byte {
	if numCodepoints == math.MaxInt32 {
		panic("unsupported: PadToMax with remote.RemoteCollation")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sql.Reset()
	c.sql.WriteString("SELECT WEIGHT_STRING(")
	c.sql.WriteString(c.prefix)
	c.hex.Write(src)
	c.sql.WriteString(c.suffix)
	if numCodepoints > 0 {
		fmt.Fprintf(&c.sql, " AS CHAR(%d)", numCodepoints)
	}
	c.sql.WriteString(")")

	result := c.performRemoteQuery()
	if result == nil {
		return nil
	}
	if dst == nil {
		return result[0].ToBytes()
	}
	return append(dst, result[0].ToBytes()...)
}

func (c *RemoteCollation) WeightStringLen(_ int) int {
	return 0
}
