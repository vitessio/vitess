/*
Copyright 2021 The Vitess Authors.

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

package credentials

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"text/template"

	"vitess.io/vitess/go/vt/grpcclient"
)

// LoadFromTemplate renders a template string into a path, using the data
// provided as the template data. It then loads the contents at the resulting
// path as a JSON file containing a grpcclient.StaticAuthClientCreds, and
// returns both the parsed credentials as well as the concrete path used.
func LoadFromTemplate(tmplStr string, data interface{}) (*grpcclient.StaticAuthClientCreds, string, error) {
	path, err := renderTemplate(tmplStr, data)
	if err != nil {
		return nil, "", err
	}

	creds, err := loadCredentials(path)
	if err != nil {
		return nil, "", err
	}

	return creds, path, nil
}

func renderTemplate(tmplStr string, data interface{}) (string, error) {
	tmpl, err := template.New("").Parse(tmplStr)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)
	if err := tmpl.Execute(buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func loadCredentials(path string) (*grpcclient.StaticAuthClientCreds, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var creds grpcclient.StaticAuthClientCreds
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, err
	}

	return &creds, nil
}
