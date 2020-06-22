package engine

import (
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Concatenate Primitive is used to concatenate results from multiple sources.
var _ Primitive = (*Concatenate)(nil)

//Concatenate specified the parameter for concatenate primitive
type Concatenate struct {
	Sources []Primitive
}

//RouteType returns a description of the query routing type used by the primitive
func (c *Concatenate) RouteType() string {
	return "Concatenate"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to
func (c *Concatenate) GetKeyspaceName() string {
	ksMap := map[string]interface{}{}
	for _, source := range c.Sources {
		ksMap[source.GetKeyspaceName()] = nil
	}
	var ksArr []string
	for ks := range ksMap {
		ksArr = append(ksArr, ks)
	}
	sort.Strings(ksArr)
	return strings.Join(ksArr, "_")
}

// GetTableName specifies the table that this primitive routes to.
func (c *Concatenate) GetTableName() string {
	var tabArr []string
	for _, source := range c.Sources {
		tabArr = append(tabArr, source.GetTableName())
	}
	return strings.Join(tabArr, "_")
}

// Execute performs a non-streaming exec.
func (c *Concatenate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	for _, source := range c.Sources {
		qr, err := source.Execute(vcursor, bindVars, wantfields)
		if err != nil {
			return nil, vterrors.Wrap(err, "Concatenate.Execute: ")
		}
		if wantfields {
			wantfields = false
			result.Fields = qr.Fields
		}
		if result.Fields != nil {
			err = compareFields(result.Fields, qr.Fields)
			if err != nil {
				return nil, err
			}
		}
		if len(qr.Rows) > 0 {
			result.Rows = append(result.Rows, qr.Rows...)
			if len(result.Rows[0]) != len(qr.Rows[0]) {
				return nil, mysql.NewSQLError(mysql.ERWrongNumberOfColumnsInSelect, "21000", "The used SELECT statements have a different number of columns")
			}
			result.RowsAffected += qr.RowsAffected
		}
	}
	return result, nil
}

// StreamExecute performs a streaming exec.
func (c *Concatenate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields fetches the field info.
func (c *Concatenate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	firstQr, err := c.Sources[0].GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	for i, source := range c.Sources {
		if i == 0 {
			continue
		}
		qr, err := source.GetFields(vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		err = compareFields(firstQr.Fields, qr.Fields)
		if err != nil {
			return nil, err
		}
	}
	return firstQr, nil
}

//NeedsTransaction returns whether a transaction is needed for this primitive
func (c *Concatenate) NeedsTransaction() bool {
	for _, source := range c.Sources {
		if source.NeedsTransaction() {
			return true
		}
	}
	return false
}

// Inputs returns the input primitives for this
func (c *Concatenate) Inputs() []Primitive {
	return c.Sources
}

func (c *Concatenate) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: c.RouteType()}
}

func compareFields(fields1 []*querypb.Field, fields2 []*querypb.Field) error {
	if len(fields1) != len(fields2) {
		return mysql.NewSQLError(mysql.ERWrongNumberOfColumnsInSelect, "21000", "The used SELECT statements have a different number of columns")
	}
	for i, field2 := range fields2 {
		field1 := fields1[i]
		if field1.Type != field2.Type {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column field type does not match for name: (%v, %v) types: (%v, %v)", field1.Name, field2.Name, field1.Type, field2.Type)
		}
	}
	return nil
}
