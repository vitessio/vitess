package vtctldclient

import (
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/credentials"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Config represents the options that modify the behavior of a Proxy.
type Config struct {
	Discovery   discovery.Discovery
	Credentials *grpcclient.StaticAuthClientCreds

	CredentialsPath string

	Cluster *vtadminpb.Cluster
}

// Parse returns a new config with the given cluster and discovery, after
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
// double-dash!) into a Config. It is meant to be called from
// (*cluster.Cluster).New().
func (c *Config) Parse(args []string) error {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)

	credentialsTmplStr := fs.String("credentials-path-tmpl", "",
		"Go template used to specify a path to a credentials file, which is a json file containing "+
			"a Username and Password. Templates are given the context of the vtctldclient.Config, "+
			"and primarily interoplate the cluster name and ID variables.")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *credentialsTmplStr != "" {
		creds, path, err := credentials.LoadFromTemplate(*credentialsTmplStr, c)
		if err != nil {
			return fmt.Errorf("cannot load credentials from path template %s: %w", *credentialsTmplStr, err)
		}

		c.CredentialsPath = path
		c.Credentials = creds
	}

	return nil
}
