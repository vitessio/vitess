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
	"encoding/json"
	"io"
	"os"
	"text/template"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/vtadmin/rbac"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type Config struct {
	Package  string           `json:"package"`
	Clusters []*ClusterConfig `json:"clusters"`
	Tests    []*Test          `json:"tests"`
}

type ClusterConfig struct {
	ID                      string                    `json:"id"`
	Name                    string                    `json:"name"`
	FakeVtctldClientResults []*FakeVtctldClientResult `json:"vtctldclient_mock_data"`
	DBTablets               []*vtadminpb.Tablet       `json:"db_tablet_list"`
}

type Test struct {
	Method         string        `json:"method"`
	Rules          []*AuthzRules `json:"rules"`
	Request        string        `json:"request"`
	SerializeCases bool          `json:"serialize_cases"`
	Cases          []*TestCase   `json:"cases"`
}

type TestCase struct {
	Name            string      `json:"name"`
	Actor           *rbac.Actor `json:"actor"`
	IsPermitted     bool        `json:"is_permitted"`
	IncludeErrorVar bool        `json:"include_error_var"`
	Assertions      []string    `json:"assertions"`
}

type AuthzRules struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
	Subjects []string `json:"subjects"`
	Clusters []string `json:"clusters"`
}

type FakeVtctldClientResult struct {
	FieldName string `json:"field"`
	Type      string `json:"type"`
	Value     string `json:"value"`
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	path := pflag.StringP("config", "c", "config.json", "authztest configuration (see the Config type in this package for the spec)")
	pflag.StringVarP(path, "config-path", "p", "config.json", "alias for --config")
	outputPath := pflag.StringP("output", "o", "", "destination to write generated code. if empty, defaults to os.Stdout")

	pflag.Parse()

	data, err := os.ReadFile(*path)
	panicIf(err)

	var cfg Config
	err = json.Unmarshal(data, &cfg)
	panicIf(err)

	tmpl, err := template.New("tests").Funcs(map[string]any{
		"getActor":       getActor,
		"writeAssertion": writeAssertion,
	}).Parse(_t)
	panicIf(err)

	var output io.Writer = os.Stdout
	if *outputPath != "" {
		f, err := os.Create(*outputPath)
		panicIf(err)

		defer f.Close()
		output = f
	}

	err = tmpl.Execute(output, &cfg)
	panicIf(err)
}
