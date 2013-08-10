// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

func TestListener(t *testing.T) {
	l, err := Listen(testPort)
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	go http.Serve(l, nil)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/debug/vars", testPort))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", string(resp))
	l.Close()
}
