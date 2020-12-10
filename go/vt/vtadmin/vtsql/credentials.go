package vtsql

import (
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
