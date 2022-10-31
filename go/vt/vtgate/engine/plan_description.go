/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"vitess.io/vitess/go/tools/graphviz"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// PrimitiveDescription is used to create a serializable representation of the Primitive tree
// Using this structure, all primitives can share json marshalling code, which gives us an uniform output
type PrimitiveDescription struct {
	OperatorType string
	Variant      string
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace
	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination
	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType
	Other            map[string]any
	Inputs           []PrimitiveDescription
}

// MarshalJSON serializes the PlanDescription into a JSON representation.
// We do this rather manual thing here so the `other` map looks like
// fields belonging to pd and not a map in a field.
func (pd PrimitiveDescription) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteString("{")

	if err := marshalAdd("", buf, "OperatorType", pd.OperatorType); err != nil {
		return nil, err
	}
	if pd.Variant != "" {
		if err := marshalAdd(",", buf, "Variant", pd.Variant); err != nil {
			return nil, err
		}
	}
	if pd.Keyspace != nil {
		if err := marshalAdd(",", buf, "Keyspace", pd.Keyspace); err != nil {
			return nil, err
		}
	}
	if pd.TargetDestination != nil {
		s := pd.TargetDestination.String()
		dest := s[11:] // TODO: All these start with Destination. We should fix that instead if trimming it out here

		if err := marshalAdd(",", buf, "TargetDestination", dest); err != nil {
			return nil, err
		}
	}
	if pd.TargetTabletType != topodatapb.TabletType_UNKNOWN {
		if err := marshalAdd(",", buf, "TargetTabletType", pd.TargetTabletType.String()); err != nil {
			return nil, err
		}
	}
	err := addMap(pd.Other, buf)
	if err != nil {
		return nil, err
	}

	if len(pd.Inputs) > 0 {
		if err := marshalAdd(",", buf, "Inputs", pd.Inputs); err != nil {
			return nil, err
		}
	}

	buf.WriteString("}")

	return buf.Bytes(), nil
}

func (pd PrimitiveDescription) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	var nodes []*graphviz.Node
	for _, input := range pd.Inputs {
		n, err := input.addToGraph(g)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	name := pd.OperatorType + ":" + pd.Variant
	if pd.Variant == "" {
		name = pd.OperatorType
	}
	this := g.AddNode(name)
	for k, v := range pd.Other {
		switch k {
		case "Query":
			this.AddTooltip(fmt.Sprintf("%v", v))
		case "FieldQuery":
		// skip these
		default:
			slice, ok := v.([]string)
			if ok {
				this.AddAttribute(k)
				for _, s := range slice {
					this.AddAttribute(s)
				}
			} else {
				this.AddAttribute(fmt.Sprintf("%s:%v", k, v))
			}
		}
	}
	for _, n := range nodes {
		g.AddEdge(this, n)
	}
	return this, nil
}

func GraphViz(p Primitive) (*graphviz.Graph, error) {
	g := graphviz.New()
	description := PrimitiveToPlanDescription(p)
	_, err := description.addToGraph(g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func addMap(input map[string]any, buf *bytes.Buffer) error {
	var mk []string
	for k, v := range input {
		if v == "" || v == nil || v == 0 {
			continue
		}
		mk = append(mk, k)
	}
	sort.Strings(mk)
	for _, k := range mk {
		v := input[k]
		if err := marshalAdd(",", buf, k, v); err != nil {
			return err
		}
	}
	return nil
}

func marshalAdd(prepend string, buf *bytes.Buffer, name string, obj any) error {
	buf.WriteString(prepend + `"` + name + `":`)

	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)

	return enc.Encode(obj)
}

// PrimitiveToPlanDescription transforms a primitive tree into a corresponding PlanDescription tree
func PrimitiveToPlanDescription(in Primitive) PrimitiveDescription {
	this := in.description()

	for _, input := range in.Inputs() {
		this.Inputs = append(this.Inputs, PrimitiveToPlanDescription(input))
	}

	if len(in.Inputs()) == 0 {
		this.Inputs = []PrimitiveDescription{}
	}

	return this
}

func orderedStringIntMap(in map[string]int) orderedMap {
	result := make(orderedMap, 0, len(in))
	for k, v := range in {
		result = append(result, keyVal{key: k, val: v})
	}
	sort.Sort(result)
	return result
}

type keyVal struct {
	key string
	val any
}

// Define an ordered, sortable map
type orderedMap []keyVal

func (m orderedMap) Len() int {
	return len(m)
}

func (m orderedMap) Less(i, j int) bool {
	return m[i].key < m[j].key
}

func (m orderedMap) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

var _ sort.Interface = (orderedMap)(nil)

func (m orderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString("{")
	for i, kv := range m {
		if i != 0 {
			buf.WriteString(",")
		}
		// marshal key
		key, err := json.Marshal(kv.key)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteString(":")
		// marshal value
		val, err := json.Marshal(kv.val)
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}

	buf.WriteString("}")
	return buf.Bytes(), nil
}
