package main

import (
	"strings"

	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

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
