package textutil

import (
	"strings"
	"text/template"
)

// ExecuteTemplate executes the given text template with the given data, and
// returns the resulting string.
func ExecuteTemplate(tmpl *template.Template, data interface{}) (string, error) {
	buf := &strings.Builder{}
	if err := tmpl.Execute(buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
