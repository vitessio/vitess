/*
Copyright 2022 The Vitess Authors.

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

package mysql

import (
	"sync"

	"vitess.io/vitess/go/vt/log"
)

const (
	setSuperUser       = `SET GLOBAL super_read_only='ON'`
	unSetSuperUser     = `SET GLOBAL super_read_only='OFF'`
	replicationDisable = "SET @@session.sql_log_bin = 0"
)

type schemaInitializerFunc struct {
	name        string
	initFunc    func(conn *Conn) error
	initialized bool
	failOnError bool
}

type schemaInitializer struct {
	funcs []*schemaInitializerFunc
	mu    sync.Mutex
}

var (
	SchemaInitializer *schemaInitializer
)

func init() {
	SchemaInitializer = newSchemaInitializer()
}
func newSchemaInitializer() *schemaInitializer {
	return &schemaInitializer{}
}

// SetSuperReadOnlyUser sets super-read-only flag to 'ON'
func (si *schemaInitializer) SetSuperReadOnlyUser(conn *Conn) error {
	if conn.IsMariaDB() {
		return nil
	}
	var err error
	log.Infof("%s", setSuperUser)
	// setting super_read_only to true, given it was set during tm_init.start()
	if _, err = conn.ExecuteFetch(setSuperUser, 0, false); err != nil {
		log.Warningf("SetSuperReadOnly(true) failed during schema initialization: %v", err)
	}

	return err
}

// UnsetSuperReadOnlyUser sets super-read-only flag to 'OFF'
func (si *schemaInitializer) UnsetSuperReadOnlyUser(conn *Conn) error {
	if conn.IsMariaDB() {
		return nil
	}

	var err error
	// setting super_read_only to true, given it was set during tm_init.start()
	log.Infof("%s", unSetSuperUser)
	if _, err = conn.ExecuteFetch(unSetSuperUser, 0, false); err != nil {
		log.Warningf("SetSuperReadOnly(true) failed schema initialization: %v", err)
	}
	return err
}

// Each function should be registered with unique name
func (si *schemaInitializer) isRegistered(name string) bool {
	for _, f := range si.funcs {
		if f.name == name {
			return true
		}
	}
	return false
}

func (si *schemaInitializer) RegisterSchemaInitializer(name string, initFunc func(conn *Conn) error, atHead bool, failOnError bool) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.isRegistered(name) {
		return
	}
	schemaFunc := &schemaInitializerFunc{name: name, initFunc: initFunc, failOnError: failOnError}
	if atHead {
		si.funcs = append([]*schemaInitializerFunc{schemaFunc}, si.funcs...)
	} else {
		si.funcs = append(si.funcs, schemaFunc)
	}
}

// TODO do we need context here
func (si *schemaInitializer) InitializeSchema(conn *Conn, disableSuperReadOnly bool, disableReplication bool) []error {
	log.Infof("Run %d initialization schemas", len(si.funcs))
	si.mu.Lock()
	defer si.mu.Unlock()

	if disableSuperReadOnly && !conn.IsMariaDB() {
		if err := si.UnsetSuperReadOnlyUser(conn); err != nil {
			log.Infof("error in setting super read-only user %s", err)
			return []error{err}
		}
		defer func() {
			if err := si.SetSuperReadOnlyUser(conn); err != nil {
				log.Infof("error in un-setting super read-only user %s", err)
			}
		}()
	}

	if disableReplication {
		if _, err := conn.ExecuteFetch(replicationDisable, 0, false); err != nil {
			log.Infof("error in disabling replication %s", err)
			return []error{err}
		}
	}

	var errors []error
	for _, f := range si.funcs {
		if f.initialized {
			log.Infof("%s is already initialized", f.name)
			continue
		}
		log.Infof("SchemaInitializer: running init function: %s", f.name)
		if err := f.initFunc(conn); err != nil {
			if f.failOnError {
				errors = append(errors, err)
			} else {
				log.Warningf("Error during schema initialization %s, continuing : %s", f.name, err)
			}
		}
		f.initialized = true
	}

	return errors
}

// getAllRegisteredFunctions is for debugging purpose
func (si *schemaInitializer) getAllRegisteredFunctions() []string {
	var functions []string
	for _, f := range si.funcs {
		functions = append(functions, f.name)
	}

	return functions
}
