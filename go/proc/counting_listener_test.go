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

func TestPublished(t *testing.T) {
	l, err := Listen("")
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	go http.Serve(l, nil)

	url := fmt.Sprintf("http://%s/debug/vars", l.Addr().String())
	for i := 1; i <= 3; i++ {
		resp, err := http.Get(url)
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
			t.Fatalf("%s response is not valid JSON. error: %v, response: %q", url, err, val)
		}
		if vars["ConnCount"].(float64) != 1 {
			t.Errorf("want 1, got %v", vars["ConnCount"])
		}
		if vars["ConnAccepted"].(float64) != float64(i) {
			t.Errorf("want %d, got %v", i, vars["ConnAccepted"])
		}
	}
	l.Close()
}
