/*
Copyright 2024 The Vitess Authors.

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

package textutil

import (
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteTemplate(t *testing.T) {
	tmplText := "Result: {{.Value}}"
	inputData := struct{ Value string }{Value: "Test"}
	tmpl, err := template.New("test").Parse(tmplText)
	require.NoError(t, err)

	result, err := ExecuteTemplate(tmpl, inputData)
	require.NoError(t, err)

	expectedResult := "Result: Test"
	assert.Equal(t, expectedResult, result)

}

func TestExecuteTemplateWithError(t *testing.T) {
	templText := "{{.UndefinedVariable}}"
	invalidInput := struct{ Name string }{Name: "foo"}

	tmpl, err := template.New("test").Parse(templText)
	require.NoError(t, err)

	result, err := ExecuteTemplate(tmpl, invalidInput)
	assert.Error(t, err)
	assert.Equal(t, "", result)
}
