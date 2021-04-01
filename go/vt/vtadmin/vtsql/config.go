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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/credentials"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Config represents the options that modify the behavior of a vtqsl.VTGateProxy.
type Config struct {
	Discovery     discovery.Discovery
	DiscoveryTags []string
	Credentials   Credentials

	DialPingTimeout time.Duration

	// CredentialsPath is used only to power vtadmin debug endpoints; there may
	// be a better way where we don't need to put this in the config, because
	// it's not really an "option" in normal use.
	CredentialsPath string

	Cluster *vtadminpb.Cluster
}

// Parse returns a new config with the given cluster ID and name, after
// attempting to parse the command-line pflags into that Config. See
// (*Config).Parse() for more details.
func Parse(cluster *vtadminpb.Cluster, disco discovery.Discovery, args []string) (*Config, error) {
	cfg := &Config{
		Cluster:   cluster,
		Discovery: disco,
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

	fs.DurationVar(&c.DialPingTimeout, "dial-ping-timeout", time.Millisecond*500,
		"Timeout to use when pinging an existing connection during calls to Dial.")
	fs.StringSliceVar(&c.DiscoveryTags, "discovery-tags", []string{},
		"repeated, comma-separated list of tags to use when discovering a vtgate to connect to. "+
			"the semantics of the tags may depend on the specific discovery implementation used")

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
