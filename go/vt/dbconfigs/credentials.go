// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconfigs

// This file contains logic for a plugable credentials system.
// The default implementation is file based.
// The flags are global, but only programs that need to access the database
// link with this library, so we should be safe.

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
)

var (
	// generic flags
	dbCredentialsServer = flag.String("db-credentials-server", "file", "db credentials server type (use 'file' for the file implementation)")

	// 'file' implementation flags
	dbCredentialsFile = flag.String("db-credentials-file", "", "db credentials file")

	// ErrUnknownUser is returned by credential server when the
	// user doesn't exist
	ErrUnknownUser = errors.New("unknown user")
)

// CredentialsServer is the interface for a credential server
type CredentialsServer interface {
	// GetUserAndPassword returns the user / password to use for a given
	// user. May return ErrUnknownUser. The user might be altered
	// to support versioned users.
	// Note this call needs to be thread safe, as we may call this from
	// multiple go routines.
	GetUserAndPassword(user string) (string, string, error)
}

// AllCredentialsServers contains all the known CredentialsServer
// implementations.  Note we will only access this after flags have
// been parsed.
var AllCredentialsServers = make(map[string]CredentialsServer)

// GetCredentialsServer returns the current CredentialsServer. Only valid
// after flag.Init was called.
func GetCredentialsServer() CredentialsServer {
	cs, ok := AllCredentialsServers[*dbCredentialsServer]
	if !ok {
		log.Fatalf("Invalid credential server: %v", *dbCredentialsServer)
	}
	return cs
}

// FileCredentialsServer is a simple implementation of CredentialsServer using
// a json file. Protected by mu.
type FileCredentialsServer struct {
	mu            sync.Mutex
	dbCredentials map[string][]string
}

// GetUserAndPassword is part of the CredentialsServer interface
func (fcs *FileCredentialsServer) GetUserAndPassword(user string) (string, string, error) {
	fcs.mu.Lock()
	defer fcs.mu.Unlock()

	if *dbCredentialsFile == "" {
		return "", "", ErrUnknownUser
	}

	// read the json file only once
	if fcs.dbCredentials == nil {
		fcs.dbCredentials = make(map[string][]string)

		data, err := ioutil.ReadFile(*dbCredentialsFile)
		if err != nil {
			log.Warningf("Failed to read dbCredentials file: %v", *dbCredentialsFile)
			return "", "", err
		}

		if err = json.Unmarshal(data, &fcs.dbCredentials); err != nil {
			log.Warningf("Failed to parse dbCredentials file: %v", *dbCredentialsFile)
			return "", "", err
		}
	}

	passwd, ok := fcs.dbCredentials[user]
	if !ok {
		return "", "", ErrUnknownUser
	}
	return user, passwd[0], nil
}

// WithCredentials returns a copy of the provided ConnParams that we can use
// to connect, after going through the CredentialsServer.
func WithCredentials(cp *sqldb.ConnParams) (sqldb.ConnParams, error) {
	result := *cp
	user, passwd, err := GetCredentialsServer().GetUserAndPassword(cp.Uname)
	switch err {
	case nil:
		result.Uname = user
		result.Pass = passwd
	case ErrUnknownUser:
		// we just use what we have, and will fail later anyway
		err = nil
	}
	return result, err
}

func init() {
	AllCredentialsServers["file"] = &FileCredentialsServer{}
}
