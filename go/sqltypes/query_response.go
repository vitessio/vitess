package sqltypes

import "reflect"

// QueryResponse represents a query response for ExecuteBatch.
type QueryResponse struct {
	QueryResult *Result
	QueryError  error
}

// QueryResponsesEqual compares two arrays of QueryResponse.
// They contain protos, so we cannot use reflect.DeepEqual.
func QueryResponsesEqual(r1, r2 []QueryResponse) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !r.QueryResult.Equal(r2[i].QueryResult) {
			return false
		}
		if !reflect.DeepEqual(r.QueryError, r2[i].QueryError) {
			return false
		}
	}
	return true
}
