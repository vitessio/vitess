// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestListener(t *testing.T) {
	l, err := Listen(testPort)
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	go http.Serve(l, nil)

	for i := 1; i <= 3; i++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/debug/vars", testPort))
		if err != nil {
			t.Fatal(err)
		}
		val, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		http.DefaultTransport.(*http.Transport).CloseIdleConnections()
		vars := make(map[string]interface{})
		err = json.Unmarshal(val, &vars)
		if err != nil {
			t.Error(err)
		}
		if vars["connection-count"].(float64) != 1 {
			t.Errorf("want 1, got %v", vars["connection-count"])
		}
		if vars["connection-accepted"].(float64) != float64(i) {
			t.Errorf("want %d, got %v", i, vars["connection-count"])
		}
	}
	l.Close()
}
