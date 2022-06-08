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
	SetSuperUser   = `SET GLOBAL super_read_only='ON'`
	UnSetSuperUser = `SET GLOBAL super_read_only='OFF'`
)

type schemaInitializerFunc struct {
	name        string
	initFunc    func() error
	initialized bool
}

type schemaInitializer struct {
	funcs       []*schemaInitializerFunc
	initialized bool
	mu          sync.Mutex
}

var SchemaInitializer *schemaInitializer

func init() {
	SchemaInitializer = newSchemaInitializer()
}
func newSchemaInitializer() *schemaInitializer {

	return &schemaInitializer{}
}

func (si *schemaInitializer) isRegistered(name string) bool {
	for _, f := range si.funcs {
		if f.name == name {
			return true
		}
	}
	return false
}

func (si *schemaInitializer) RegisterSchemaInitializer(name string, initFunc func() error, atHead bool) error {
	si.mu.Lock()
	log.Infof("SchemaInitializer: registering function: %s %d", name, len(si.funcs))
	defer si.mu.Unlock()
	if si.isRegistered(name) {
		return nil
	}
	if si.initialized {
		if err := initFunc(); err != nil {
			return err
		}
	} else {
		initFunc := &schemaInitializerFunc{name: name, initFunc: initFunc}
		if atHead {
			si.funcs = append([]*schemaInitializerFunc{initFunc}, si.funcs...)
		} else {
			si.funcs = append(si.funcs, initFunc)
		}
	}
	return nil
}

func (si *schemaInitializer) InitializeSchema() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.initialized {
		return nil
	}

	for _, f := range si.funcs {
		if f.initialized {
			continue
		}
		log.Infof("SchemaInitializer: running init function: %s", f.name)
		if err := f.initFunc(); err != nil && !f.initialized {
			log.Infof("error: %s", err)
			return err
		}
		f.initialized = true
	}
	return nil
}
