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

import "fmt"

// Transform takes an input file and collects the information into an easier to consume format
func Transform(input *SourceFile) *SourceInformation {
	interestingTypes := make(map[string]Type)
	interfaces := make(map[string]bool)
	structs := make(map[string]*StructDeclaration)
	typeAliases := make(map[string]*TypeAlias)

	for _, l := range input.lines {
		switch line := l.(type) {
		case *FuncDeclaration:
			interestingTypes[line.receiver.typ.toTypString()] = line.receiver.typ
		case *StructDeclaration:
			structs[line.name] = line
		case *TypeAlias:
			typeAliases[line.name] = line
		case *InterfaceDeclaration:
			interfaces[line.name] = true
		}
	}

	return &SourceInformation{
		interfaces:       interfaces,
		interestingTypes: interestingTypes,
		structs:          structs,
		typeAliases:      typeAliases,
	}
}

// SourceInformation contains the information from the ast.go file, but in a format that is easier to consume
type SourceInformation struct {
	interestingTypes map[string]Type
	interfaces       map[string]bool
	structs          map[string]*StructDeclaration
	typeAliases      map[string]*TypeAlias
}

func (v *SourceInformation) String() string {
	var types string
	for _, k := range v.interestingTypes {
		types += k.toTypString() + "\n"
	}
	var structs string
	for _, k := range v.structs {
		structs += k.toSastString() + "\n"
	}
	var typeAliases string
	for _, k := range v.typeAliases {
		typeAliases += k.toSastString() + "\n"
	}

	return fmt.Sprintf("Types to build visitor for:\n%s\nStructs with fields: \n%s\nTypeAliases with type: \n%s\n", types, structs, typeAliases)
}

// getItemTypeOfArray will return nil if the given type is not pointing to a array type.
// If it is an array type, the type of it's items will be returned
func (v *SourceInformation) getItemTypeOfArray(typ Type) Type {
	alias := v.typeAliases[typ.rawTypeName()]
	if alias == nil {
		return nil
	}
	arrTyp, isArray := alias.typ.(*Array)
	if !isArray {
		return v.getItemTypeOfArray(alias.typ)
	}
	return arrTyp.inner
}

func (v *SourceInformation) isSQLNode(typ Type) bool {
	_, isInteresting := v.interestingTypes[typ.toTypString()]
	if isInteresting {
		return true
	}
	_, isInterface := v.interfaces[typ.toTypString()]
	return isInterface
}
