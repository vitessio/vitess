package vtsql

import (
	"github.com/spf13/pflag"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
)

// Config represents the options that modify the behavior of a vtqsl.VTGateProxy.
type Config struct {
	Discovery     discovery.Discovery
	DiscoveryTags []string
	Credentials   Credentials
}

// Parse reads options specified as command-line pflags (--key=value, note the
// double-dash!) into a vtsql.Config. It is meant to be called from
// (*cluster.Cluster).New().
func (c *Config) Parse(args []string) error {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)

	creds := credentialsFlag{}

	fs.StringSliceVar(&c.DiscoveryTags, "discovery-tags", []string{},
		"repeated, comma-separated list of tags to use when discovering a vtgate to connect to. "+
			"the semantics of the tags may depend on the specific discovery implementation used")
	fs.Var(&creds, "credentials-path", "path to a json file containing a Username and Password")
	effectiveUser := fs.String("effective-username", "", "username to send queries on behalf of")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// If we loaded credentials, but did not receive an effective user, then the
	// immediate user is the effective user.
	if *effectiveUser == "" && creds.parsed {
		*effectiveUser = creds.Username
	}

	c.Credentials = &StaticAuthCredentials{
		EffectiveUser:         *effectiveUser,
		StaticAuthClientCreds: creds.StaticAuthClientCreds,
	}

	return nil
}
