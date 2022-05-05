/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"crypto/sha256"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Result represents a query result.
type Result struct {
	Fields              []*querypb.Field `json:"fields"`
	RowsAffected        uint64           `json:"rows_affected"`
	RowsRead            uint64           `json:"rows_read"`
	InsertID            uint64           `json:"insert_id"`
	Rows                []Row            `json:"rows"`
	SessionStateChanges string           `json:"session_state_changes"`
	StatusFlags         uint16           `json:"status_flags"`
}

//goland:noinspection GoUnusedConst
const (
	ServerStatusInTrans            = 0x0001
	ServerStatusAutocommit         = 0x0002
	ServerMoreResultsExists        = 0x0008
	ServerStatusNoGoodIndexUsed    = 0x0010
	ServerStatusNoIndexUsed        = 0x0020
	ServerStatusCursorExists       = 0x0040
	ServerStatusLastRowSent        = 0x0080
	ServerStatusDbDropped          = 0x0100
	ServerStatusNoBackslashEscapes = 0x0200
	ServerStatusMetadataChanged    = 0x0400
	ServerQueryWasSlow             = 0x0800
	ServerPsOutParams              = 0x1000
	ServerStatusInTransReadonly    = 0x2000
	ServerSessionStateChanged      = 0x4000
)

// ResultStream is an interface for receiving Result. It is used for
// RPC interfaces.
type ResultStream interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() (*Result, error)
}

// Repair fixes the type info in the rows
// to conform to the supplied field types.
func (result *Result) Repair(fields []*querypb.Field) {
	// Usage of j is intentional.
	for j, f := range fields {
		for _, r := range result.Rows {
			if r[j].typ != Null {
				r[j].typ = f.Type
			}
		}
	}
}

// ReplaceKeyspace replaces all the non-empty Database identifiers in the result
// set with the given keyspace name
func (result *Result) ReplaceKeyspace(keyspace string) {
	// Change database name in mysql output to the keyspace name
	for _, f := range result.Fields {
		if f.Database != "" {
			f.Database = keyspace
		}
	}
}

// Copy creates a deep copy of Result.
func (result *Result) Copy() *Result {
	out := &Result{
		InsertID:     result.InsertID,
		RowsAffected: result.RowsAffected,
		RowsRead:     result.RowsRead,
	}
	if result.Fields != nil {
		out.Fields = make([]*querypb.Field, len(result.Fields))
		for i, f := range result.Fields {
			out.Fields[i] = proto.Clone(f).(*querypb.Field)
		}
	}
	if result.Rows != nil {
		out.Rows = make([][]Value, 0, len(result.Rows))
		for _, r := range result.Rows {
			out.Rows = append(out.Rows, CopyRow(r))
		}
	}
	return out
}

// CopyRow makes a copy of the row.
func CopyRow(r []Value) []Value {
	// The raw bytes of the values are supposed to be treated as read-only.
	// So, there's no need to copy them.
	out := make([]Value, len(r))
	copy(out, r)
	return out
}

// Truncate returns a new Result with all the rows truncated
// to the specified number of columns.
func (result *Result) Truncate(l int) *Result {
	if l == 0 {
		return result
	}

	out := &Result{
		InsertID:     result.InsertID,
		RowsAffected: result.RowsAffected,
		RowsRead:     result.RowsRead,
	}
	if result.Fields != nil {
		out.Fields = result.Fields[:l]
	}
	if result.Rows != nil {
		out.Rows = make([][]Value, 0, len(result.Rows))
		for _, r := range result.Rows {
			out.Rows = append(out.Rows, r[:l])
		}
	}
	return out
}

// FieldsEqual compares two arrays of fields.
// reflect.DeepEqual shouldn't be used because of the protos.
func FieldsEqual(f1, f2 []*querypb.Field) bool {
	if len(f1) != len(f2) {
		return false
	}
	for i, f := range f1 {
		if !proto.Equal(f, f2[i]) {
			return false
		}
	}
	return true
}

// Equal compares the Result with another one.
// reflect.DeepEqual shouldn't be used because of the protos.
func (result *Result) Equal(other *Result) bool {
	// Check for nil cases
	if result == nil {
		return other == nil
	}
	if other == nil {
		return false
	}

	// Compare Fields, RowsAffected, InsertID, Rows.
	return FieldsEqual(result.Fields, other.Fields) &&
		result.RowsAffected == other.RowsAffected &&
		result.InsertID == other.InsertID &&
		reflect.DeepEqual(result.Rows, other.Rows)
}

// ResultsEqual compares two arrays of Result.
// reflect.DeepEqual shouldn't be used because of the protos.
func ResultsEqual(r1, r2 []Result) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !r.Equal(&r2[i]) {
			return false
		}
	}
	return true
}

// ResultsEqualUnordered compares two unordered arrays of Result.
func ResultsEqualUnordered(r1, r2 []Result) bool {
	if len(r1) != len(r2) {
		return false
	}

	// allRows is a hash map that contains a row hashed as a key and
	// the number of occurrence as the value. we use this map to ensure
	// equality between the two result sets. when analyzing r1, we
	// increment each key's value by one for each row's occurrence, and
	// then we decrement it by one each time we see the same key in r2.
	// if one of the key's value is not equal to zero, then r1 and r2 do
	// not match.
	allRows := map[string]int{}
	countRows := 0
	for _, r := range r1 {
		saveRowsAnalysis(r, allRows, &countRows, true)
	}
	for _, r := range r2 {
		saveRowsAnalysis(r, allRows, &countRows, false)
	}
	if countRows != 0 {
		return false
	}
	for _, i := range allRows {
		if i != 0 {
			return false
		}
	}
	return true
}

func saveRowsAnalysis(r Result, allRows map[string]int, totalRows *int, increment bool) {
	for _, row := range r.Rows {
		newHash := hashCodeForRow(row)
		if increment {
			allRows[newHash]++
		} else {
			allRows[newHash]--
		}
	}
	if increment {
		*totalRows += int(r.RowsAffected)
	} else {
		*totalRows -= int(r.RowsAffected)
	}
}

func hashCodeForRow(val []Value) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", val)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

// MakeRowTrusted converts a *querypb.Row to []Value based on the types
// in fields. It does not sanity check the values against the type.
// Every place this function is called, a comment is needed that explains
// why it's justified.
func MakeRowTrusted(fields []*querypb.Field, row *querypb.Row) []Value {
	sqlRow := make([]Value, len(row.Lengths))
	var offset int64
	for i, length := range row.Lengths {
		if length < 0 {
			continue
		}
		sqlRow[i] = MakeTrusted(fields[i].Type, row.Values[offset:offset+length])
		offset += length
	}
	return sqlRow
}

// IncludeFieldsOrDefault normalizes the passed Execution Options.
// It returns the default value if options is nil.
func IncludeFieldsOrDefault(options *querypb.ExecuteOptions) querypb.ExecuteOptions_IncludedFields {
	if options == nil {
		return querypb.ExecuteOptions_TYPE_AND_NAME
	}

	return options.IncludedFields
}

// StripMetadata will return a new Result that has the same Rows,
// but the Field objects will have their non-critical metadata emptied.  Note we don't
// proto.Copy each Field for performance reasons, but we only copy the
// individual fields.
func (result *Result) StripMetadata(incl querypb.ExecuteOptions_IncludedFields) *Result {
	if incl == querypb.ExecuteOptions_ALL || len(result.Fields) == 0 {
		return result
	}
	r := *result
	r.Fields = make([]*querypb.Field, len(result.Fields))
	newFieldsArray := make([]querypb.Field, len(result.Fields))
	for i, f := range result.Fields {
		r.Fields[i] = &newFieldsArray[i]
		newFieldsArray[i].Type = f.Type
		if incl == querypb.ExecuteOptions_TYPE_AND_NAME {
			newFieldsArray[i].Name = f.Name
		}
	}
	return &r
}

// AppendResult will combine the Results Objects of one result
// to another result.Note currently it doesn't handle cases like
// if two results have different fields.We will enhance this function.
func (result *Result) AppendResult(src *Result) {
	if src.RowsAffected == 0 && len(src.Rows) == 0 && len(src.Fields) == 0 {
		return
	}
	if result.Fields == nil {
		result.Fields = src.Fields
	}
	result.RowsAffected += src.RowsAffected
	result.RowsRead += src.RowsRead
	if src.InsertID != 0 {
		result.InsertID = src.InsertID
	}
	result.Rows = append(result.Rows, src.Rows...)
}

// Named returns a NamedResult based on this struct
func (result *Result) Named() *NamedResult {
	return ToNamedResult(result)
}

// IsMoreResultsExists returns true if the status flag has SERVER_MORE_RESULTS_EXISTS set
func (result *Result) IsMoreResultsExists() bool {
	return result.StatusFlags&ServerMoreResultsExists == ServerMoreResultsExists
}

// IsInTransaction returns true if the status flag has SERVER_STATUS_IN_TRANS set
func (result *Result) IsInTransaction() bool {
	return result.StatusFlags&ServerStatusInTrans == ServerStatusInTrans
}
