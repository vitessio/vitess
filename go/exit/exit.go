/*
Copyright 2017 Google Inc.

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

/*
Package exit provides an alternative to os.Exit(int).

Unlike os.Exit(int), exit.Return(int) will run deferred functions before
terminating. It's effectively like a return from main(), except you can specify
the exit code.

Defer a call to exit.Recover() or exit.RecoverAll() at the beginning of main().
Use exit.Return(int) to initiate an exit.

	func main() {
		defer exit.Recover()
		defer cleanup()
		...
		if err != nil {
			// Return from main() with a non-zero exit code,
			// making sure to run deferred cleanup.
			exit.Return(1)
		}
		...
	}

All functions deferred *after* defer exit.Recover()/RecoverAll() will be
executed before the exit. This is why the defer for this package should
be the first statement in main().

NOTE: This mechanism only works if exit.Return() is called from the same
goroutine that deferred exit.Recover(). Usually this means Return() should
only be used from within main(), or within functions that are only ever
called from main(). See Recover() and Return() for more details.
*/
package exit

import (
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/tb"
)

type exitCode int

var (
	exitFunc = os.Exit // can be faked out for testing
)

// Recover should be deferred as the first line of main(). It recovers the
// panic initiated by Return and converts it to a call to os.Exit. Any
// functions deferred after Recover in the main goroutine will be executed
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
