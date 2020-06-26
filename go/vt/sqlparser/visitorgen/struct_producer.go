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

package visitorgen

import (
	"fmt"
	"sort"
)

// VisitorData is the data needed to produce the output file
type (
	// VisitorItem represents something that needs to be added to the rewriter infrastructure
	VisitorItem interface {
		toFieldItemString() string
		typeName() string
		asSwitchCase() string
		asReplMethod() string
		getFieldName() string
	}

	// SingleFieldItem is a single field in a struct
	SingleFieldItem struct {
		StructType, FieldType Type
		FieldName             string
	}

	// ArrayFieldItem is an array field in a struct
	ArrayFieldItem struct {
		StructType, ItemType Type
		FieldName            string
	}

	// ArrayItem is an array that implements SQLNode
	ArrayItem struct {
		StructType, ItemType Type
	}

	// VisitorPlan represents all the output needed for the rewriter
	VisitorPlan struct {
		Switches []*SwitchCase // The cases for the big switch statement used to implement the visitor
	}

	// SwitchCase is what we need to know to produce all the type switch cases in the visitor.
	SwitchCase struct {
		Type   Type
		Fields []VisitorItem
	}
)

var _ VisitorItem = (*SingleFieldItem)(nil)
var _ VisitorItem = (*ArrayItem)(nil)
var _ VisitorItem = (*ArrayFieldItem)(nil)
var _ sort.Interface = (*VisitorPlan)(nil)
var _ sort.Interface = (*SwitchCase)(nil)

// ToVisitorPlan transforms the source information into a plan for the visitor code that needs to be produced
func ToVisitorPlan(input *SourceInformation) *VisitorPlan {
	var output VisitorPlan

	for _, typ := range input.interestingTypes {
		switchit := &SwitchCase{Type: typ}
		stroct, isStruct := input.structs[typ.rawTypeName()]
		if isStruct {
			for _, f := range stroct.fields {
				switchit.Fields = append(switchit.Fields, trySingleItem(input, f, typ)...)
			}
		} else {
			itemType := input.getItemTypeOfArray(typ)
			if itemType != nil && input.isSQLNode(itemType) {
				switchit.Fields = append(switchit.Fields, &ArrayItem{
					StructType: typ,
					ItemType:   itemType,
				})
			}
		}
		sort.Sort(switchit)
		output.Switches = append(output.Switches, switchit)
	}
	sort.Sort(&output)
	return &output
}

func trySingleItem(input *SourceInformation, f *Field, typ Type) []VisitorItem {
	if input.isSQLNode(f.typ) {
		return []VisitorItem{&SingleFieldItem{
			StructType: typ,
			FieldType:  f.typ,
			FieldName:  f.name,
		}}
	}

	arrType, isArray := f.typ.(*Array)
	if isArray && input.isSQLNode(arrType.inner) {
		return []VisitorItem{&ArrayFieldItem{
			StructType: typ,
			ItemType:   arrType.inner,
			FieldName:  f.name,
		}}
	}
	return []VisitorItem{}
}

// String returns a string, used for testing
func (v *VisitorPlan) String() string {
	var sb builder
	for _, s := range v.Switches {
		sb.appendF("Type: %v", s.Type.toTypString())
		for _, f := range s.Fields {
			sb.appendF("\t%v", f.toFieldItemString())
		}
	}
	return sb.String()
}

func (s *SingleFieldItem) toFieldItemString() string {
	return fmt.Sprintf("single item: %v of type: %v", s.FieldName, s.FieldType.toTypString())
}

func (s *SingleFieldItem) asSwitchCase() string {
	return fmt.Sprintf(`		a.apply(node, n.%s, %s)`, s.FieldName, s.typeName())
}

func (s *SingleFieldItem) asReplMethod() string {
	_, isRef := s.StructType.(*Ref)

	if isRef {
		return fmt.Sprintf(`func %s(newNode, parent SQLNode) {
	parent.(%s).%s = newNode.(%s)
}`, s.typeName(), s.StructType.toTypString(), s.FieldName, s.FieldType.toTypString())
	}

	return fmt.Sprintf(`func %s(newNode, parent SQLNode) {
	tmp := parent.(%s)
	tmp.%s = newNode.(%s)
}`, s.typeName(), s.StructType.toTypString(), s.FieldName, s.FieldType.toTypString())

}

func (ai *ArrayItem) asReplMethod() string {
	name := ai.typeName()
	return fmt.Sprintf(`type %s int

func (r *%s) replace(newNode, container SQLNode) {
	container.(%s)[int(*r)] = newNode.(%s)
}

func (r *%s) inc() {
	*r++
}`, name, name, ai.StructType.toTypString(), ai.ItemType.toTypString(), name)
}

func (afi *ArrayFieldItem) asReplMethod() string {
	name := afi.typeName()
	return fmt.Sprintf(`type %s int

func (r *%s) replace(newNode, container SQLNode) {
	container.(%s).%s[int(*r)] = newNode.(%s)
}

func (r *%s) inc() {
	*r++
}`, name, name, afi.StructType.toTypString(), afi.FieldName, afi.ItemType.toTypString(), name)
}

func (s *SingleFieldItem) getFieldName() string {
	return s.FieldName
}

func (s *SingleFieldItem) typeName() string {
	return "replace" + s.StructType.rawTypeName() + s.FieldName
}

func (afi *ArrayFieldItem) toFieldItemString() string {
	return fmt.Sprintf("array field item: %v.%v contains items of type %v", afi.StructType.toTypString(), afi.FieldName, afi.ItemType.toTypString())
}

func (ai *ArrayItem) toFieldItemString() string {
	return fmt.Sprintf("array item: %v containing items of type %v", ai.StructType.toTypString(), ai.ItemType.toTypString())
}

func (ai *ArrayItem) getFieldName() string {
	panic("Should not be called!")
}

func (afi *ArrayFieldItem) getFieldName() string {
	return afi.FieldName
}

func (ai *ArrayItem) asSwitchCase() string {
	return fmt.Sprintf(`		replacer := %s(0)
		replacerRef := &replacer
		for _, item := range n {
			a.apply(node, item, replacerRef.replace)
			replacerRef.inc()
		}`, ai.typeName())
}

func (afi *ArrayFieldItem) asSwitchCase() string {
	return fmt.Sprintf(`		replacer%s := %s(0)
		replacer%sB := &replacer%s
		for _, item := range n.%s {
			a.apply(node, item, replacer%sB.replace)
			replacer%sB.inc()
		}`, afi.FieldName, afi.typeName(), afi.FieldName, afi.FieldName, afi.FieldName, afi.FieldName, afi.FieldName)
}

func (ai *ArrayItem) typeName() string {
	return "replace" + ai.StructType.rawTypeName() + "Items"
}

func (afi *ArrayFieldItem) typeName() string {
	return "replace" + afi.StructType.rawTypeName() + afi.FieldName
}
func (v *VisitorPlan) Len() int {
	return len(v.Switches)
}

func (v *VisitorPlan) Less(i, j int) bool {
	return v.Switches[i].Type.rawTypeName() < v.Switches[j].Type.rawTypeName()
}

func (v *VisitorPlan) Swap(i, j int) {
	temp := v.Switches[i]
	v.Switches[i] = v.Switches[j]
	v.Switches[j] = temp
}
func (s *SwitchCase) Len() int {
	return len(s.Fields)
}

func (s *SwitchCase) Less(i, j int) bool {
	return s.Fields[i].getFieldName() < s.Fields[j].getFieldName()
}

func (s *SwitchCase) Swap(i, j int) {
	temp := s.Fields[i]
	s.Fields[i] = s.Fields[j]
	s.Fields[j] = temp
}
