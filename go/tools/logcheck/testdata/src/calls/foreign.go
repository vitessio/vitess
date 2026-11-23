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

package calls

import "otherlogger"

// This file does not import go/vt/log. The logger is obtained elsewhere;
// the callee is still *log.PrefixedLogger.Error.
func foreign(name string) {
	otherlogger.Logger().Error("ok")
	otherlogger.Logger().Error(name) // want "log message must be a constant; pass values as slog attributes"
}
