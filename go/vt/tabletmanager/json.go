// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
)

func toJson(x interface{}) string {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(data)
}
