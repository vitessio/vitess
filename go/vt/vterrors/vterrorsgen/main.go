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
	"log"
	"os"
	"strings"
	"text/template"

	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/vt/vterrors"
)

const (
	tmpl = `
| ID | Description | Error | MySQL Error Code | SQL State |
| --- | --- | --- | --- | --- |
{{- range $err := . }}
{{- $data := (call $err) }}
| {{ $data.ID }} | {{ $data.Description }} | {{ FormatError $data.Err }} | {{ ConvertStateToMySQLErrorCode $data.State }} | {{ ConvertStateToMySQLState $data.State }} |
{{- end }}
`
)

// This program reads the errors located in the `vitess.io/vitess/go/vt/vterrors` package
// and prints on the standard output a table, in Markdown format, that lists all the
// errors with their code, description, error content, mysql error code and the SQL state.
func main() {
	t := template.New("template")
	t.Funcs(map[string]any{
		"ConvertStateToMySQLErrorCode": sqlerror.ConvertStateToMySQLErrorCode,
		"ConvertStateToMySQLState":     sqlerror.ConvertStateToMySQLState,
		"FormatError": func(err error) string {
			s := err.Error()
			return strings.TrimSpace(strings.Join(strings.Split(s, ":")[1:], ":"))
		},
	})
	t = template.Must(t.Parse(tmpl))

	err := t.ExecuteTemplate(os.Stdout, "template", vterrors.Errors)
	if err != nil {
		log.Fatal(err)
	}
}
