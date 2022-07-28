/*
Copyright 2022 The Vitess Authors.

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

package main

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

func formatDocRow(m *DocMethod) string {
	var buf strings.Builder
	buf.WriteString("| `")
	buf.WriteString(m.Name)
	buf.WriteString("` | ")

	for i, r := range m.Rules {
		fmt.Fprintf(&buf, "`(%s, %s)`", r.Action, r.Resource)
		if i != len(m.Rules)-1 {
			buf.WriteString(", ")
		}
	}

	buf.WriteString(" |")

	return buf.String()
}

func getActor(actor *rbac.Actor) string {
	var buf strings.Builder
	switch actor {
	case nil:
		buf.WriteString("var actor *rbac.Actor")
	default:
		buf.WriteString("actor := ")
		_inlineActor(&buf, *actor)
	}

	return buf.String()
}

func _inlineActor(buf *strings.Builder, actor rbac.Actor) {
	buf.WriteString(`&rbac.Actor{Name: "`)
	buf.WriteString(actor.Name)
	buf.WriteString(`"`)
	if actor.Roles != nil {
		buf.WriteString(", Roles: []string{")
		for i, role := range actor.Roles {
			buf.WriteString(strings.Join([]string{`"`, role, `"`}, ""))
			if i != len(actor.Roles)-1 {
				buf.WriteString(", ")
			}
		}

		buf.WriteString("}")
	}

	buf.WriteString("}")
}

func writeAssertion(line string, test *Test, testCase *TestCase) string {
	if !strings.Contains(line, "$$") {
		return line
	}

	var msg strings.Builder
	msg.WriteString(`"actor %+v should `)
	if !testCase.IsPermitted {
		msg.WriteString("not ")
	}

	msg.WriteString("be permitted to ")
	msg.WriteString(test.Method)
	msg.WriteString(`", actor`)

	return strings.ReplaceAll(line, "$$", msg.String())
}
