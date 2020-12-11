package vtsql

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/grpcclient"
)

func TestConfigParse(t *testing.T) {
	cfg := Config{}

	// This asserts we do not attempt to load a credentialsFlag via its Set func
	// if it's not specified in the args slice.
	err := cfg.Parse([]string{})
	assert.NoError(t, err)

	t.Run("", func(t *testing.T) {
		f, err := ioutil.TempFile("", "vtsql-config-test-testcluster-*") // testcluster is going to appear in the template
		require.NoError(t, err)

		_, err = f.Write([]byte(`{
	"Username": "vtadmin",
	"Password": "hunter2"
}`))
		require.NoError(t, err)

		path := f.Name()
		defer os.Remove(path)
		f.Close()

		dir := filepath.Dir(path)
		baseParts := strings.Split(filepath.Base(path), "-")
		tmplParts := append(baseParts[:3], "{{ .ClusterName }}", baseParts[4])

		cfg := &Config{
			ClusterName: "testcluster",
		}

		credsTmplStr := filepath.Join(dir, strings.Join(tmplParts, "-"))

		args := []string{
			"--discovery-tags=a:1,b:2",
			"--effective-user=vt_appdebug",
			"--discovery-tags=c:3",
			fmt.Sprintf("--credentials-path-tmpl=%s", credsTmplStr),
		}

		expectedCreds := &StaticAuthCredentials{
			EffectiveUser: "vt_appdebug",
			StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
				Username: "vtadmin",
				Password: "hunter2",
			},
		}
		expectedTags := []string{
			"a:1",
			"b:2",
			"c:3",
		}

		err = cfg.Parse(args)
		assert.NoError(t, err)
		assert.Equal(t, expectedTags, cfg.DiscoveryTags)
		assert.Equal(t, expectedCreds, cfg.Credentials)
	})
}
