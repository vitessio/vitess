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

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		impl string
		err  error
		typ  Discovery
	}{
		{
			name: "success",
			impl: "consul",
			err:  nil,
			typ:  &ConsulDiscovery{},
		},
		{
			name: "unregistered",
			impl: "unregistered",
			err:  ErrImplementationNotRegistered,
			typ:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disco, err := New(tt.impl, &vtadminpb.Cluster{Id: "testid", Name: "testcluster"}, []string{})
			if tt.err != nil {
				assert.Error(t, err, tt.err.Error())
				return
			}

			assert.NoError(t, err)
			assert.IsType(t, tt.typ, disco)
		})
	}
}

func TestRegister(t *testing.T) {
	Register("testfactory", nil)

	defer func() {
		err := recover()
		assert.NotNil(t, err)
		assert.IsType(t, "", err)
		assert.Contains(t, err.(string), "factory already registered")
	}()

	// this one panics
	Register("testfactory", nil)
	assert.Equal(t, 1, 2, "double register should have panicked")
}
