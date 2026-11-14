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

package constants

import (
	"errors"
	"fmt"
	"log/slog"

	"example.com/consts"
	"vitess.io/vitess/go/vt/log"
)

const local = "local message"

type code string

const codeConst code = "from-code"

func accepted(name string) {
	log.Info("literal", slog.String("name", name))
	log.Info(local)
	log.Info(consts.Prefix)
	log.Info(("parenthesized"))
	log.Info(consts.Prefix + "ended")
	log.Info("a" + "b")
	log.Info(string(codeConst))
}

func rejected(name string) {
	err := errors.New("x")
	log.Info("table " + name)              // want "log message must be a constant; pass values as slog attributes"
	log.Error(fmt.Sprintf("bad %s", name)) // want "log message must be a constant; pass values as slog attributes"
	log.Error(err.Error())                 // want "log message must be a constant; pass values as slog attributes"
	log.Warn(name)                         // want "log message must be a constant; pass values as slog attributes"
}

func callForms(name string) {
	defer log.Info("ok")
	defer log.Info(name) // want "log message must be a constant; pass values as slog attributes"
	go log.Warn(name)    // want "log message must be a constant; pass values as slog attributes"
	(log.Error)("ok")
	(log.Error)(name)                                 // want "log message must be a constant; pass values as slog attributes"
	log.InfoDepth(1, name, slog.String("name", name)) // want "log message must be a constant; pass values as slog attributes"
}

func depthAndMessage() (int, string) { return 1, "ok" }

func messageAndAttr() (string, slog.Attr) { return "ok", slog.Int("n", 1) }

// A multi-value call supplies every argument, so none of them is a constant.
func multiValue() {
	log.DebugDepth(depthAndMessage()) // want "log message must be a constant; pass values as slog attributes"
	log.Info(messageAndAttr())        // want "log message must be a constant; pass values as slog attributes"
}
