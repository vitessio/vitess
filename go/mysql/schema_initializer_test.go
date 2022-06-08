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
	"testing"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
)

func TestSchemaInitializer(t *testing.T) {
	func1 := func() error { return nil }
	func2 := func() error { return vterrors.Errorf(vtrpc.Code_ABORTED, "test aborted ...") }

	if err := SchemaInitializer.RegisterSchemaInitializer("func1", func1, false); err != nil {
		assert.FailNow(t, "Error should not be nil")
	}

	if err := SchemaInitializer.RegisterSchemaInitializer("func2", func2, false); err != nil {
		assert.FailNow(t, "Error should not be nil")
	}

	err := SchemaInitializer.InitializeSchema()
	assert.EqualErrorf(t, err, "test aborted ...", "Error mismatch")

	SchemaInitializer.initialized = true

	if err := SchemaInitializer.RegisterSchemaInitializer("func3", func1, false); err != nil {
		assert.FailNow(t, "Error should not be nil")
	}

	if err := SchemaInitializer.RegisterSchemaInitializer("func4", func2, false); err != nil {
		assert.EqualErrorf(t, err, "test aborted ...", "Error mismatch")
	} else {
		assert.FailNow(t, "Error should not be nil")
	}

}
