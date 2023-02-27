package vtgate

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
)

func suggestTabletType(inTransaction bool, sql string) (tabletType topodatapb.TabletType, err error) {
	suggestedTabletType := defaultTabletType
	if inTransaction {
		return suggestedTabletType, nil
	}
	// if not in transaction, and the query is read-only, use REPLICA
	ro, err := IsReadOnly(sql)
	if err != nil {
		return suggestedTabletType, err
	}
	if ro {
		suggestedTabletType = topodatapb.TabletType_REPLICA
	}
	return suggestedTabletType, nil
}

// IsReadOnly : whether the query should be routed to a read-only vttablet
func IsReadOnly(query string) (bool, error) {
	s, _, err := sqlparser.Parse2(query)
	if err != nil {
		return false, err
	}
	// select last_insert_id() is a special case, it's not a read-only query
	if sqlparser.ContainsLastInsertIDStatement(s) {
		return false, nil
	}
	return sqlparser.IsPureSelectStatement(s), nil
}

func IsReadOnlyStmt(stmt sqlparser.Statement) (bool, error) {
	return sqlparser.IsPureSelectStatement(stmt), nil
}
