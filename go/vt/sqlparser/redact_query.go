package sqlparser

import querypb "vitess.io/vitess/go/vt/proto/query"

// RedactSQLQuery returns a sql string with the params stripped out for display
func RedactSQLQuery(sql string) (string, error) {
	bv := map[string]*querypb.BindVariable{}
	sqlStripped, comments := SplitMarginComments(sql)

	stmt, reservedVars, err := Parse2(sqlStripped)
	if err != nil {
		return "", err
	}

	err = Normalize(stmt, NewReservedVars("redacted", reservedVars), bv)
	if err != nil {
		return "", err
	}

	return comments.Leading + String(stmt) + comments.Trailing, nil
}
