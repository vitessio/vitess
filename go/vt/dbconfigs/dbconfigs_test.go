// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconfigs

import "testing"

func TestRegisterFlagsWithoutFlags(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("RegisterFlags should panic with empty db flags")
		}
	}()
	dbConfigs = DBConfigs{}
	RegisterFlags(EmptyConfig)
}

func TestRegisterFlagsWithSomeFlags(t *testing.T) {
	dbConfigs = DBConfigs{}
	registeredFlags := RegisterFlags(DbaConfig | ReplConfig)
	if registeredFlags&AppConfig != 0 {
		t.Error("App connection params should not be registered.")
	}
	if registeredFlags&DbaConfig == 0 {
		t.Error("Dba connection params should be registered.")
	}
	if registeredFlags&FilteredConfig != 0 {
		t.Error("Filtered connection params should not be registered.")
	}
	if registeredFlags&ReplConfig == 0 {
		t.Error("Repl connection params should be registered.")
	}
}

func TestInitWithEmptyFlags(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Init should panic with empty db flags")
		}
	}()
	Init("", EmptyConfig)
}
