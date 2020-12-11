package vtsql

import (
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

func loadCredentials(path string) (*grpcclient.StaticAuthClientCreds, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var creds grpcclient.StaticAuthClientCreds
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, err
	}

	return &creds, nil
}
