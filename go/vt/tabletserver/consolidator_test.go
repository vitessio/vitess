package tabletserver

import (
	"testing"

	"github.com/youtube/vitess/go/mysql/proto"
)

func TestConsolidator(t *testing.T) {
	qe := NewQueryEngine(qsConfig)
	sql := "select * from SomeTable"

	orig, added := qe.consolidator.Create(sql)
	if !added {
		t.Errorf("expected consolidator to register a new entry")
	}

	dup, added := qe.consolidator.Create(sql)
	if added {
		t.Errorf("did not expect consolidator to register a new entry")
	}

	go func() {
		orig.Result = &proto.QueryResult{InsertId: 145}
		orig.Broadcast()
	}()
	dup.Wait()

	if orig.Result.InsertId != dup.Result.InsertId {
		t.Errorf("failed to share the result")
	}

	// Running the query again should add a new entry since the original
	// query execution completed
	_, added = qe.consolidator.Create(sql)
	if !added {
		t.Errorf("expected consolidator to register a new entry")
	}
}
