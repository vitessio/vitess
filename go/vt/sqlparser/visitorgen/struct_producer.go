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
		toFieldItemString(container Type) string
		replaceMethodName(container Type) string
		asSwitchCase(container Type) string
		asReplMethod(container Type) string
		getFieldName() string
		//getCloneMethod() string
	}

	// SingleFieldItem is a single field in a struct
	SingleFieldItem struct {
		FieldType Type
		FieldName string
	}

	// ArrayFieldItem is an array field in a struct
	ArrayFieldItem struct {
		ItemType  Type
		FieldName string
	}

	// ArrayItem is an array that implements SQLNode
	ArrayItem struct {
		ItemType Type
	}

	// VisitorPlan represents all the output needed for the rewriter
	VisitorPlan struct {
		ASTTypes []*ASTType
	}

	// ASTType is what we need to know to produce all the type switch cases in the visitor.
	ASTType struct {
		Type   Type
		Fields []VisitorItem
	}
)

var _ VisitorItem = (*SingleFieldItem)(nil)
var _ VisitorItem = (*ArrayItem)(nil)
var _ VisitorItem = (*ArrayFieldItem)(nil)
var _ sort.Interface = (*VisitorPlan)(nil)
var _ sort.Interface = (*ASTType)(nil)

// ToVisitorPlan transforms the source information into a plan for the visitor code that needs to be produced
func ToVisitorPlan(input *SourceInformation) *VisitorPlan {
	var output VisitorPlan

	for _, typ := range input.interestingTypes {
		switchit := &ASTType{Type: typ}
		stroct, isStruct := input.structs[typ.rawTypeName()]
		if isStruct {
			for _, f := range stroct.fields {
				switchit.Fields = append(switchit.Fields, trySingleItem(input, f, typ)...)
			}
		} else {
			itemType := input.getItemTypeOfArray(typ)
			if itemType != nil && input.isSQLNode(itemType) {
				switchit.Fields = append(switchit.Fields, &ArrayItem{
					ItemType: itemType,
				})
			}
		}
		sort.Sort(switchit)
		output.ASTTypes = append(output.ASTTypes, switchit)
	}
	sort.Sort(&output)
	return &output
}

func trySingleItem(input *SourceInformation, f *Field, typ Type) []VisitorItem {
	if input.isSQLNode(f.typ) {
		return []VisitorItem{&SingleFieldItem{
			FieldType: f.typ,
			FieldName: f.name,
		}}
	}

	arrType, isArray := f.typ.(*Array)
	if isArray && input.isSQLNode(arrType.inner) {
		return []VisitorItem{&ArrayFieldItem{
			ItemType:  arrType.inner,
			FieldName: f.name,
		}}
	}
	return []VisitorItem{}
}

// String returns a string, used for testing
func (v *VisitorPlan) String() string {
	var sb builder
	for _, s := range v.ASTTypes {
		sb.appendF("Type: %v", s.Type.toTypeString())
		for _, f := range s.Fields {
			sb.appendF("\t%v", f.toFieldItemString(s.Type))
		}
	}
	return sb.String()
}

func (s *SingleFieldItem) toFieldItemString(_ Type) string {
	return fmt.Sprintf("single item: %v of type: %v", s.FieldName, s.FieldType.toTypeString())
}

func (s *SingleFieldItem) asSwitchCase(container Type) string {
	return fmt.Sprintf(`		a.apply(node, n.%s, %s)`, s.FieldName, s.replaceMethodName(container))
}

func (s *SingleFieldItem) asReplMethod(container Type) string {
	_, isRef := container.(*Ref)

	if isRef {
		return fmt.Sprintf(`func %s(newNode, parent SQLNode) {
	parent.(%s).%s = newNode.(%s)
}`, s.replaceMethodName(container), container.toTypeString(), s.FieldName, s.FieldType.toTypeString())
	}

	return fmt.Sprintf(`func %s(newNode, parent SQLNode) {
	tmp := parent.(%s)
	tmp.%s = newNode.(%s)
}`, s.replaceMethodName(container), container.toTypeString(), s.FieldName, s.FieldType.toTypeString())

}

func (ai *ArrayItem) asReplMethod(container Type) string {
	name := ai.replaceMethodName(container)
	return fmt.Sprintf(`type %s int

func (r *%s) replace(newNode, container SQLNode) {
	container.(%s)[int(*r)] = newNode.(%s)
}

func (r *%s) inc() {
	*r++
}`, name, name, container.toTypeString(), ai.ItemType.toTypeString(), name)
}

func (afi *ArrayFieldItem) asReplMethod(container Type) string {
	name := afi.replaceMethodName(container)
	return fmt.Sprintf(`type %s int

func (r *%s) replace(newNode, container SQLNode) {
	container.(%s).%s[int(*r)] = newNode.(%s)
}

func (r *%s) inc() {
	*r++
}`, name, name, container.toTypeString(), afi.FieldName, afi.ItemType.toTypeString(), name)
}

func (s *SingleFieldItem) getFieldName() string {
	return s.FieldName
}

func (s *SingleFieldItem) replaceMethodName(container Type) string {
	return "replace" + container.rawTypeName() + s.FieldName
}

func (afi *ArrayFieldItem) toFieldItemString(container Type) string {
	return fmt.Sprintf("array field item: %v.%v contains items of type %v", container.toTypeString(), afi.FieldName, afi.ItemType.toTypeString())
}

func (ai *ArrayItem) toFieldItemString(container Type) string {
	return fmt.Sprintf("array item: %v containing items of type %v", container.toTypeString(), ai.ItemType.toTypeString())
}

func (ai *ArrayItem) getFieldName() string {
	panic("Should not be called!")
}

func (afi *ArrayFieldItem) getFieldName() string {
	return afi.FieldName
}

func (ai *ArrayItem) asSwitchCase(container Type) string {
	return fmt.Sprintf(`		replacer := %s(0)
		replacerRef := &replacer
		for _, item := range n {
			a.apply(node, item, replacerRef.replace)
			replacerRef.inc()
		}`, ai.replaceMethodName(container))
}

func (afi *ArrayFieldItem) asSwitchCase(container Type) string {
	return fmt.Sprintf(`		replacer%s := %s(0)
		replacer%sB := &replacer%s
		for _, item := range n.%s {
			a.apply(node, item, replacer%sB.replace)
			replacer%sB.inc()
		}`, afi.FieldName, afi.replaceMethodName(container), afi.FieldName, afi.FieldName, afi.FieldName, afi.FieldName, afi.FieldName)
}

func (ai *ArrayItem) replaceMethodName(container Type) string {
	return "replace" + container.rawTypeName() + "Items"
}

func (afi *ArrayFieldItem) replaceMethodName(container Type) string {
	return "replace" + container.rawTypeName() + afi.FieldName
}
func (v *VisitorPlan) Len() int {
	return len(v.ASTTypes)
}

func (v *VisitorPlan) Less(i, j int) bool {
	return v.ASTTypes[i].Type.rawTypeName() < v.ASTTypes[j].Type.rawTypeName()
}

func (v *VisitorPlan) Swap(i, j int) {
	temp := v.ASTTypes[i]
	v.ASTTypes[i] = v.ASTTypes[j]
	v.ASTTypes[j] = temp
}
func (s *ASTType) Len() int {
	return len(s.Fields)
}

func (s *ASTType) Less(i, j int) bool {
	return s.Fields[i].getFieldName() < s.Fields[j].getFieldName()
}

func (s *ASTType) Swap(i, j int) {
	temp := s.Fields[i]
	s.Fields[i] = s.Fields[j]
	s.Fields[j] = temp
}

//func (s *ASTType) toCloneString() string {
//	nilCheck := ""
//	_, isRef := s.Type.(*Ref)
//	if isRef {
//		nilCheck =
//			`	if in == nil {
//		return nil
//	}`
//	}
//	return fmt.Sprintf(`func (n %s) clone() SQLNode {
//%s
//}`, s.Type.toTypeString(), nilCheck)
//}
