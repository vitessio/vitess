/*
Copyright 2020 The Vitess Authors.

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

package vtsql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/backoff"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestConfigParse(t *testing.T) {
	t.Parallel()

	cfg := Config{}

	// This asserts we do not attempt to load a credentialsFlag via its Set func
	// if it's not specified in the args slice.
	err := cfg.Parse([]string{})
	assert.NoError(t, err)

	t.Run("", func(t *testing.T) {
		t.Parallel()

		f, err := os.CreateTemp("", "vtsql-config-test-testcluster-*") // testcluster is going to appear in the template
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
		tmplParts := append(baseParts[:3], "{{ .Cluster.Name }}", baseParts[4])

		cfg := &Config{
			Cluster: &vtadminpb.Cluster{
				Name: "testcluster",
			},
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
		assert.Equal(t, expectedTags, cfg.ResolverOptions.DiscoveryTags)
		assert.Equal(t, expectedCreds, cfg.Credentials)
	})

	t.Run("uses vtsql-credentials-password", func(t *testing.T) {
		t.Parallel()

		f, err := os.CreateTemp("", "vtsql-config-test-testcluster-*") // testcluster is going to appear in the template
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
		tmplParts := append(baseParts[:3], "{{ .Cluster.Name }}", baseParts[4])

		cfg := &Config{
			Cluster: &vtadminpb.Cluster{
				Name: "testcluster",
			},
		}

		credsTmplStr := filepath.Join(dir, strings.Join(tmplParts, "-"))

		args := []string{
			"--discovery-tags=a:1,b:2",
			"--effective-user=vt_appdebug",
			"--discovery-tags=c:3",
			"--credentials-password=my_password",
			fmt.Sprintf("--credentials-path-tmpl=%s", credsTmplStr),
		}

		expectedCreds := &StaticAuthCredentials{
			EffectiveUser: "vt_appdebug",
			StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
				Username: "vtadmin",
				Password: "my_password",
			},
		}
		expectedTags := []string{
			"a:1",
			"b:2",
			"c:3",
		}

		err = cfg.Parse(args)
		assert.NoError(t, err)
		assert.Equal(t, expectedTags, cfg.ResolverOptions.DiscoveryTags)
		assert.Equal(t, expectedCreds, cfg.Credentials)
	})

	t.Run("it uses vtsql credentials passed as flags", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Cluster: &vtadminpb.Cluster{
				Name: "testcluster",
			},
		}

		args := []string{
			"--discovery-tags=a:1,b:2",
			"--effective-user=vt_appdebug",
			"--discovery-tags=c:3",
			"--credentials-username=vtadmin",
			"--credentials-password=my_password",
		}

		expectedCreds := &StaticAuthCredentials{
			EffectiveUser: "vt_appdebug",
			StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
				Username: "vtadmin",
				Password: "my_password",
			},
		}
		expectedTags := []string{
			"a:1",
			"b:2",
			"c:3",
		}

		err = cfg.Parse(args)
		assert.NoError(t, err)
		assert.Equal(t, expectedTags, cfg.ResolverOptions.DiscoveryTags)
		assert.Equal(t, expectedCreds, cfg.Credentials)
	})

	t.Run("", func(t *testing.T) {
		t.Parallel()

		f, err := os.CreateTemp("", "vtsql-config-test-testcluster-*") // testcluster is going to appear in the template
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
		tmplParts := append(baseParts[:3], "{{ .Cluster.Name }}", baseParts[4])

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

		expected := &Config{
			Cluster: &vtadminpb.Cluster{
				Id:   "cid",
				Name: "testcluster",
			},
			ResolverOptions: &resolver.Options{
				DiscoveryTags:        expectedTags,
				DiscoveryTimeout:     100 * time.Millisecond,
				MinDiscoveryInterval: time.Second * 30,
				BackoffConfig:        backoff.DefaultConfig,
			},
			Credentials:     expectedCreds,
			CredentialsPath: path,
		}

		cfg, err := Parse(&vtadminpb.Cluster{Id: "cid", Name: "testcluster"}, nil, args)
		assert.NoError(t, err)
		assert.Equal(t, expected, cfg)
	})
}
