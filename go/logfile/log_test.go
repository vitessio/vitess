// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package logfile

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	if os.Getenv("RUN_MANUAL_TEST") == "" {
		fmt.Println("skipping logfile test. Set RUN_MANUAL_TEST to something to run it")
		t.SkipNow()
	}
	m, err := Open("tfile1", 0, 0, 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	for i := 0; i < 5; i++ {
		s := fmt.Sprintf("log %d\n", i)
		m.Write([]byte(s))
	}

	m, err = Open("tfile2", 60, 10, 10)
	if err != nil {
		t.Errorf("%v", err)
	}
	for i := 0; i < 60; i++ {
		<-time.After(1e9)
		s := fmt.Sprintf("log %d\n", i)
		m.Write([]byte(s))
	}
	m.Close()

}
