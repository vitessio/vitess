package tabletserver

import (
	"testing"

	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func TestQueryList(t *testing.T) {
	ql := NewQueryList(nil)
	connID := int64(1)
	qd := NewQueryDetail(&proto.Query{}, &context.DummyContext{}, connID)
	ql.Add(qd)

	if qd1, ok := ql.queryDetails[connID]; !ok || qd1.connID != connID {
		t.Errorf("failed to add to QueryList")
	}

	conn2ID := int64(2)
	qd2 := NewQueryDetail(&proto.Query{}, &context.DummyContext{}, conn2ID)
	ql.Add(qd2)

	rows := ql.GetQueryzRows()
	if len(rows) != 2 || rows[0].ConnID != 1 || rows[1].ConnID != 2 {
		t.Errorf("wrong rows returned %v", rows)
	}

	ql.Remove(qd)
	if _, ok := ql.queryDetails[connID]; ok {
		t.Errorf("failed to remove from QueryList")
	}
}
