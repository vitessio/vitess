package vtsql

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	"google.golang.org/grpc/credentials"
	"vitess.io/vitess/go/vt/grpcclient"
)

// Credentials defines the interface needed for vtsql properly connect to and
// query Vitess databases.
type Credentials interface {
	// GetEffectiveUsername returns the username on whose behalf the DB is
	// issuing queries.
	GetEffectiveUsername() string
	// GetUsername returns the immediate username for a DB connection.
	GetUsername() string
	credentials.PerRPCCredentials
}

// StaticAuthCredentials augments a grpcclient.StaticAuthClientCreds with an
// effective username.
type StaticAuthCredentials struct {
	*grpcclient.StaticAuthClientCreds
	EffectiveUser string
}

var _ Credentials = (*StaticAuthCredentials)(nil)

// GetEffectiveUsername is part of the Credentials interface.
func (creds *StaticAuthCredentials) GetEffectiveUsername() string {
	return creds.EffectiveUser
}

// GetUsername is part of the Credentials interface.
func (creds *StaticAuthCredentials) GetUsername() string {
	return creds.Username
}

// credentialsFlag adds the pflag.Value interface to a
// grpcclient.StaticAuthClientCreds struct, and is used in (*Config).Parse().
// The effective user component of vtsql's StaticAuthCredentials is parsed
// separately.
type credentialsFlag struct {
	*grpcclient.StaticAuthClientCreds
	parsed bool
}

func (cf *credentialsFlag) Set(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, cf); err != nil {
		return err
	}

	cf.parsed = true

	return nil
}

func (cf *credentialsFlag) String() string {
	buf := bytes.NewBuffer(nil)

	buf.WriteString("&grpcclient.StaticAuthClientCreds{Username:")
	buf.WriteString(cf.Username)
	buf.WriteString(", Password: ******}") // conditionally show this value

	return buf.String()
}

func (cf *credentialsFlag) Type() string {
	return "vtsql.credentialsFlag"
}
