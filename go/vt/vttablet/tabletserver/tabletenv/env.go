/*
Copyright 2019 The Vitess Authors.

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

// Package tabletenv maintains environment variables and types that
// are common for all packages of tabletserver.
package tabletenv

import (
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

// Env defines the functions supported by TabletServer
// that the sub-componennts need to access.
type Env interface {
	CheckMySQL()
	Config() *TabletConfig
	Exporter() *servenv.Exporter
	Stats() *Stats
	LogError()
}

type testEnv struct {
	config   *TabletConfig
	exporter *servenv.Exporter
	stats    *Stats
}

// NewEnv creates an Env that can be used for tabletserver subcomponents
// without an actual TabletServer.
func NewEnv(config *TabletConfig, exporterName string) Env {
	exporter := servenv.NewExporter(exporterName, "Tablet")
	return &testEnv{
		config:   config,
		exporter: exporter,
		stats:    NewStats(exporter),
	}
}

func (*testEnv) CheckMySQL()                    {}
func (te *testEnv) Config() *TabletConfig       { return te.config }
func (te *testEnv) Exporter() *servenv.Exporter { return te.exporter }
func (te *testEnv) Stats() *Stats               { return te.stats }

func (te *testEnv) LogError() {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		te.Stats().InternalErrors.Add("Panic", 1)
	}
}
