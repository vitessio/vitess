/*
Copyright 2026 The Vitess Authors.

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

package log

import (
	"log/slog"
	"sync/atomic"
)

// ErrorAttrsFunc adds attributes to r that describe v, the value r logs under
// the "error" key.
//
// It runs synchronously for every enabled record of the structured backend,
// with the first top-level "error" attribute only (not one in a group or added
// with slog.Logger.With). It must be cheap, must not log, and must not keep r.
type ErrorAttrsFunc func(r *slog.Record, v slog.Value)

var errorAttrs atomic.Pointer[ErrorAttrsFunc]

// SetErrorAttrs sets the function called for every structured record that is
// logged with an "error" attribute, and returns the previous one.
// go/mysql/sqlerror sets it to add the MySQL error number, since this package
// can't import sqlerror.
func SetErrorAttrs(fn ErrorAttrsFunc) ErrorAttrsFunc {
	var prev *ErrorAttrsFunc
	if fn == nil {
		prev = errorAttrs.Swap(nil)
	} else {
		prev = errorAttrs.Swap(&fn)
	}
	if prev == nil {
		return nil
	}
	return *prev
}

// addErrorAttrs calls the ErrorAttrsFunc with the first "error" attribute
// in attrs.
func addErrorAttrs(r *slog.Record, attrs []slog.Attr) {
	for _, attr := range attrs {
		if attr.Key != "error" {
			continue
		}
		if fn := errorAttrs.Load(); fn != nil {
			(*fn)(r, attr.Value)
		}
		return
	}
}
