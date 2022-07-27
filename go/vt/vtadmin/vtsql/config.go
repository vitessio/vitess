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
	"database/sql"
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"
	"vitess.io/vitess/go/vt/vtadmin/credentials"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Config represents the options that modify the behavior of a vtqsl.VTGateProxy.
type Config struct {
	Credentials Credentials
	// CredentialsPath is used only to power vtadmin debug endpoints; there may
	// be a better way where we don't need to put this in the config, because
	// it's not really an "option" in normal use.
	CredentialsPath string

	Cluster         *vtadminpb.Cluster
	ResolverOptions *resolver.Options

	dialFunc func(c vitessdriver.Configuration) (*sql.DB, error)
}

// ConfigOption is a function that mutates a Config. It should return the same
// Config structure, in a builder-pattern style.
type ConfigOption func(cfg *Config) *Config

// WithDialFunc returns a ConfigOption that applies the given dial function to
// a Config.
//
// It is used to support dependency injection in tests, and needs to be exported
// for higher-level tests (for example, package vtadmin/cluster).
func WithDialFunc(f func(c vitessdriver.Configuration) (*sql.DB, error)) ConfigOption {
	return func(cfg *Config) *Config {
		cfg.dialFunc = f
		return cfg
	}
}

// Parse returns a new config with the given cluster ID and name, after
// attempting to parse the command-line pflags into that Config. See
// (*Config).Parse() for more details.
func Parse(cluster *vtadminpb.Cluster, disco discovery.Discovery, args []string) (*Config, error) {
	cfg := &Config{
		Cluster: cluster,
		ResolverOptions: &resolver.Options{
			Discovery: disco,
		},
	}

	err := cfg.Parse(args)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// Parse reads options specified as command-line pflags (--key=value, note the
// double-dash!) into a vtsql.Config. It is meant to be called from
// (*cluster.Cluster).New().
func (c *Config) Parse(args []string) error {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)

	if c.ResolverOptions == nil {
		c.ResolverOptions = &resolver.Options{}
	}

	c.ResolverOptions.InstallFlags(fs)

	credentialsTmplStr := fs.String("credentials-path-tmpl", "",
		"Go template used to specify a path to a credentials file, which is a json file containing "+
			"a Username and Password. Templates are given the context of the vtsql.Config, and primarily "+
			"interoplate the cluster name and ID variables.")
	effectiveUser := fs.String("effective-user", "", "username to send queries on behalf of")

	if err := fs.Parse(args); err != nil {
		return err
	}

	var creds *grpcclient.StaticAuthClientCreds

	if *credentialsTmplStr != "" {
		_creds, path, err := credentials.LoadFromTemplate(*credentialsTmplStr, c)
		if err != nil {
			return fmt.Errorf("cannot load credentials from path template %s: %w", *credentialsTmplStr, err)
		}

		c.CredentialsPath = path
		creds = _creds
	}

	if creds != nil {
		// If we did not receive an effective user, but loaded credentials, then the
		// immediate user is the effective user.
		if *effectiveUser == "" {
			*effectiveUser = creds.Username
		}

		c.Credentials = &StaticAuthCredentials{
			EffectiveUser:         *effectiveUser,
			StaticAuthClientCreds: creds,
		}
	}

	return nil
}
