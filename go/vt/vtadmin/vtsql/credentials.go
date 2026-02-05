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
