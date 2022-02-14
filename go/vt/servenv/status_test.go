package servenv

import (
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

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

func TestNamedStatus(t *testing.T) {
	server := httptest.NewServer(nil)
	defer server.Close()

	name := "test"
	sp := newStatusPage(name)
	sp.addStatusFuncs(
		template.FuncMap{
			"github_com_vitessio_vitess_to_upper": strings.ToUpper,
		})

	sp.addStatusPart("test_part", `{{github_com_vitessio_vitess_to_upper . }}`, func() interface{} {
		return "this should be uppercase"
	})
	sp.addStatusSection("test_section", func() string {
		return "this is a section"
	})

	resp, err := http.Get(server.URL + "/" + name + StatusURLPath())
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

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
