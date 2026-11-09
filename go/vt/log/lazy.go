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

import "log/slog"

// LazyString defers building an expensive attribute value, such as proto text
// or JSON, until the record is written, so records that are dropped (a V(n)
// level that is off, or a throttled logger) don't pay for it.
//
//	log.V(2).Info("Updated vschema", slog.Any("vschema", log.LazyString(func() string { ... })))
type LazyString func() string

func (f LazyString) LogValue() slog.Value {
	return slog.StringValue(f())
}

// String returns the value for loggers that format attributes with fmt, such
// as the glog backend.
func (f LazyString) String() string {
	return f()
}
