// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconfigs

// This file contains logic for a plugable credentials system.
// The default implementation is file based.
// The flags are global, but only programs that need to acess the database
// link with this library, so we should be safe.

import (
	"errors"
	"flag"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
)

var (
	// generic flags
	dbCredentialsServer = flag.String("db-credentials-server", "file", "db credentials server type (use 'file' for the file implementation)")

	// 'file' implementation flags
	dbCredentialsFile = flag.String("db-credentials-file", "", "db credentials file")

	// error returned by credential server when the user doesn't exist
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

	// GetSubprocessFlags returns the flags to send to a subprocess
	// to initialize the exact same CredentialsServer
	GetSubprocessFlags() []string
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

// getCredentialsServerSubprocessFlags returns the flags to use for
// sub-processes
func getCredentialsServerSubprocessFlags() []string {
	result := []string{
		"-db-credentials-server", *dbCredentialsServer,
	}
	cs := GetCredentialsServer()
	result = append(result, cs.GetSubprocessFlags()...)
	return result
}

// FileCredentialsServer is a simple implementation of CredentialsServer using
// a json file. Protected by mu.
type FileCredentialsServer struct {
	mu            sync.Mutex
	dbCredentials map[string][]string
}

func (fcs *FileCredentialsServer) GetUserAndPassword(user string) (string, string, error) {
	fcs.mu.Lock()
	defer fcs.mu.Unlock()

	if *dbCredentialsFile == "" {
		return "", "", ErrUnknownUser
	}

	// read the json file only once
	if fcs.dbCredentials == nil {
		fcs.dbCredentials = make(map[string][]string)
		if err := jscfg.ReadJson(*dbCredentialsFile, &fcs.dbCredentials); err != nil {
			log.Warningf("Failed to read dbCredentials file: %v", *dbCredentialsFile)
			return "", "", err
		}
	}

	if passwd, ok := fcs.dbCredentials[user]; !ok {
		return "", "", ErrUnknownUser
	} else {
		return user, passwd[0], nil
	}
}

func (fcs *FileCredentialsServer) GetSubprocessFlags() []string {
	return []string{
		"-db-credentials-file", *dbCredentialsFile,
	}
}

func init() {
	AllCredentialsServers["file"] = &FileCredentialsServer{}
}
