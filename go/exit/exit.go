// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package exit provides an alternative to os.Exit(int) that executes deferred
functions before exiting.

Defer a call to exit.Recover() or exit.RecoverAll() at the beginning of main().
Use exit.Return(int) to initiate an exit.

	func main() {
		defer exit.Recover()
		...
		f()
	}

	func f() {
		exit.Return(123)
	}

This package should only be used from the main goroutine.
*/
package exit

import (
	"os"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/tb"
)

type exitCode int

var (
	exitFunc = os.Exit // can be faked out for testing
)

// Recover should be deferred as the first line of main(). It recovers the
// panic initiated by Return and converts it to a call to os.Exit. Any
// functions deferred after Recover in the main goroutine should be executed
// prior to exiting. Recover will re-panic anything other than the panic it
// expects from Return.
func Recover() {
	doRecover(recover(), false)
}

// RecoverAll can be deferred instead of Recover as the first line of main().
// Instead of re-panicking, RecoverAll will absorb any panic and convert it to
// an error log entry with a stack trace, followed by a call to os.Exit(255).
func RecoverAll() {
	doRecover(recover(), true)
}

func doRecover(err interface{}, recoverAll bool) {
	if err == nil {
		return
	}

	switch code := err.(type) {
	case exitCode:
		exitFunc(int(code))
	default:
		if recoverAll {
			log.Errorf("panic: %v", tb.Errorf("%v", err))
			exitFunc(255)
		} else {
			panic(err)
		}
	}
}

// Return initiates a panic that sends the return code to the deferred Recover,
// executing other deferred functions along the way. When the panic reaches
// Recover, the return code will be passed to os.Exit. This should only be
// called from the main goroutine.
func Return(code int) {
	panic(exitCode(code))
}
