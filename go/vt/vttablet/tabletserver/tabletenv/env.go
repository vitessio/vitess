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
