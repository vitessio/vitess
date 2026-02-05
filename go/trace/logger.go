/*
Copyright 2021 The Vitess Authors.

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

package trace

import "vitess.io/vitess/go/vt/log"

// traceLogger wraps the standard vitess log package to satisfy the datadog and
// jaeger logger interfaces.
type traceLogger struct{}

// Log is part of the ddtrace.Logger interface. Datadog only ever logs errors.
func (*traceLogger) Log(msg string) { log.Errorf(msg) }

// Error is part of the jaeger.Logger interface.
func (*traceLogger) Error(msg string) { log.Errorf(msg) }

// Infof is part of the jaeger.Logger interface.
func (*traceLogger) Infof(msg string, args ...any) { log.Infof(msg, args...) }
