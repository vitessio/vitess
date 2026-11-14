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

package nottracked

import (
	"log/slog"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

// None of these end in (msg string, attrs ...slog.Attr), so none are logging
// functions.
func signatures(name string, attrs []slog.Attr) {
	log.Group(len(name))
	log.Attrs(slog.String("name", name))
	log.InfoSlice(name, attrs)
	log.InfoValues(name, slog.StringValue(name))
	log.InfoMessage(log.Message(name))
	log.InfoAttrs(name, log.Attr{Key: "name", Value: name})
}

// Only go/vt/log's NewPrefixedLogger has its prefix checked.
func otherPrefixedLogger(name string) {
	logutil.NewPrefixedLogger(name).Info("ok")
	mk := logutil.NewPrefixedLogger
	mk(name)
}
