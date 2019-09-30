/*
Copyright 2017 Google Inc.

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

package dbconfigs

// This file contains logic for a pluggable credentials system.
// The default implementation is file based.
// The flags are global, but only programs that need to access the database
// link with this library, so we should be safe.

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

var (
	// generic flags
	dbCredentialsServer = flag.String("db-credentials-server", "file", "db credentials server type (use 'file' for the file implementation)")

	// 'file' implementation flags
	dbCredentialsFile = flag.String("db-credentials-file", "", "db credentials file; send SIGHUP to reload this file")

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
		log.Exitf("Invalid credential server: %v", *dbCredentialsServer)
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
func WithCredentials(cp *mysql.ConnParams) (*mysql.ConnParams, error) {
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
	return &result, err
}

func init() {
	AllCredentialsServers["file"] = &FileCredentialsServer{}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for range sigChan {
			if fcs, ok := AllCredentialsServers["file"].(*FileCredentialsServer); ok {
				fcs.mu.Lock()
				fcs.dbCredentials = nil
				fcs.mu.Unlock()
			}
		}
	}()
}
