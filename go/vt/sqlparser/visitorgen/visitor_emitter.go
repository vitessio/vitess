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
	"strings"
)

// EmitReplacementMethods is an anti-parser (a.k.a prettifier) - it takes a struct that is much like an AST,
// and produces a string from it. This method will produce the replacement methods that make it possible to
// replace objects in fields or in slices.
func EmitReplacementMethods(vd *VisitorPlan) string {
	var sb builder
	for _, s := range vd.Switches {
		for _, k := range s.Fields {
			sb.appendF(k.asReplMethod())
			sb.newLine()
		}
	}

	return sb.String()
}

// EmitTypeSwitches is an anti-parser (a.k.a prettifier) - it takes a struct that is much like an AST,
// and produces a string from it. This method will produce the switch cases needed to cover the Vitess AST.
func EmitTypeSwitches(vd *VisitorPlan) string {
	var sb builder
	for _, s := range vd.Switches {
		sb.newLine()
		sb.appendF("	case %s:", s.Type.toTypString())
		for _, k := range s.Fields {
			sb.appendF(k.asSwitchCase())
		}
	}

	return sb.String()
}

func (b *builder) String() string {
	return strings.TrimSpace(b.sb.String())
}

type builder struct {
	sb strings.Builder
}

func (b *builder) appendF(format string, data ...interface{}) *builder {
	_, err := b.sb.WriteString(fmt.Sprintf(format, data...))
	if err != nil {
		panic(err)
	}
	b.newLine()
	return b
}

func (b *builder) newLine() {
	_, err := b.sb.WriteString("\n")
	if err != nil {
		panic(err)
	}
}
