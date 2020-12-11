package vtsql

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/spf13/pflag"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
)

// Config represents the options that modify the behavior of a vtqsl.VTGateProxy.
type Config struct {
	Discovery     discovery.Discovery
	DiscoveryTags []string
	Credentials   Credentials

	// CredentialsPath is used only to power vtadmin debug endpoints; there may
	// be a better way where we don't need to put this in the config, because
	// it's not really an "option" in normal use.
	CredentialsPath string

	ClusterID   string
	ClusterName string
}

// Parse reads options specified as command-line pflags (--key=value, note the
// double-dash!) into a vtsql.Config. It is meant to be called from
// (*cluster.Cluster).New().
func (c *Config) Parse(args []string) error {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)

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
		_creds, path, err := c.loadCredentialsFromTemplate(*credentialsTmplStr)
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

func (c Config) loadCredentialsFromTemplate(tmplStr string) (*grpcclient.StaticAuthClientCreds, string, error) {
	path, err := c.renderTemplate(tmplStr)
	if err != nil {
		return nil, "", err
	}

	creds, err := loadCredentials(path)

	return creds, path, err
}

func (c Config) renderTemplate(tmplStr string) (string, error) {
	tmpl, err := template.New("").Parse(tmplStr)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)
	if err := tmpl.Execute(buf, &c); err != nil {
		return "", err
	}

	return buf.String(), nil
}
