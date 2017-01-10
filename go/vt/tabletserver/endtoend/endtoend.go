// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package endtoend is a test-only package. It runs various
// end-to-end tests on tabletserver.
package endtoend

import (
	"encoding/json"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
)

func prettyPrint(qr sqltypes.Result) string {
	out, err := json.Marshal(qr)
	if err != nil {
		log.Errorf("Could not marshal result to json for %#v", qr)
		return fmt.Sprintf("%#v", qr)
	}
	return string(out)
}

func prettyPrintArr(qr []sqltypes.Result) string {
	out, err := json.Marshal(qr)
	if err != nil {
		log.Errorf("Could not marshal result to json for %#v", qr)
		return fmt.Sprintf("%#v", qr)
	}
	return string(out)
}
