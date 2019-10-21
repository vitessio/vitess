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

package servenv

import (
	"html/template"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func init() {
	AddStatusFuncs(
		template.FuncMap{
			"github_com_vitessio_vitess_to_upper": strings.ToUpper,
		})

	AddStatusPart("test_part", `{{github_com_vitessio_vitess_to_upper . }}`, func() interface{} {
		return "this should be uppercase"
	})
	AddStatusSection("test_section", func() string {
		return "this is a section"
	})
}

func TestStatus(t *testing.T) {
	server := httptest.NewServer(nil)
	defer server.Close()

	resp, err := http.Get(server.URL + StatusURLPath())
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ioutil.ReadAll: %v", err)
	}

	cases := []string{
		`h1.*test_part.*/h1`,
		`THIS SHOULD BE UPPERCASE`,
		`h1.*test_section.*/h1`,
	}
	for _, cas := range cases {
		if !regexp.MustCompile(cas).Match(body) {
			t.Errorf("failed matching: %q", cas)
		}
	}
	t.Logf("body: \n%s", body)
}
