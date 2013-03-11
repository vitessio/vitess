// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconfigs

import (
	"encoding/json"
	"testing"
)

func TestDbConfigLoad(t *testing.T) {

	data := "{\n" +
		"  \"keyrange\": {\n" +
		"    \"Start\": \"4000\",\n" +
		"    \"End\": \"8000\"\n" +
		"  }\n" +
		"}"

	val := &DBConfig{}
	if err := json.Unmarshal([]byte(data), val); err != nil {
		t.Errorf("json.Unmarshal error: %v", err)
	}
	if string(val.KeyRange.Start.Hex()) != "4000" {
		t.Errorf("Invalid Start: %v", val.KeyRange.Start.Hex())
	}
	if string(val.KeyRange.End.Hex()) != "8000" {
		t.Errorf("Invalid End: %v", val.KeyRange.End.Hex())
	}
}
