package engine

import (
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*Concatenate)(nil)

type Concatenate struct {
	Sources []Primitive
}

func (c Concatenate) RouteType() string {
	return "Concatenate"
}

func (c Concatenate) GetKeyspaceName() string {
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

func (c Concatenate) GetTableName() string {
	var tabArr []string
	for _, source := range c.Sources {
		tabArr = append(tabArr, source.GetTableName())
	}
	return strings.Join(tabArr, "_")
}

func (c Concatenate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (c Concatenate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (c Concatenate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
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
		for i, field := range qr.Fields {
			if firstQr.Fields[i].Type != field.Type {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column field type does not match for name: (%v, %v) types: (%v, %v)", firstQr.Fields[i].Name, field.Name, firstQr.Fields[i].Type, field.Type)
			}
		}
	}
	return firstQr, nil
}

func (c Concatenate) NeedsTransaction() bool {
	for _, source := range c.Sources {
		if source.NeedsTransaction() {
			return true
		}
	}
	return false
}

func (c Concatenate) Inputs() []Primitive {
	return c.Sources
}

func (c Concatenate) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: c.RouteType()}
}
