
package schema

import (
	"fmt"
	"testing"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func TestSerialization(t *testing.T) {
	m := map[string]*schema.Table{
		"foo":&schema.Table{
			Name:         sqlparser.NewTableIdent("oh noes"),
			Fields:       []*querypb.Field{{
				Name:                 "column",
				Type:                 querypb.Type_VARCHAR,
			}},
			PKColumns:    nil,
			Type:         0,
			SequenceInfo: nil,
			MessageInfo:  nil,
		},
	}

	b := ToGOB(m)
	newMap := FromGOB(b)
	fmt.Printf("%v", newMap)
}