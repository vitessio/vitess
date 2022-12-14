package logutil

import (
	pslog "github.com/planetscale/log"
	noglog "github.com/slok/noglog"

	"vitess.io/vitess/go/vt/log"
)

type PSLogger pslog.SugaredLogger

// SetPlanetScaleLogger in-place noglog replacement with PlanetScales' logger.
func SetPlanetScaleLogger(conf *pslog.Config) (psLogger *pslog.SugaredLogger) {

	if conf == nil {
		configLogger, _ := conf.Build()
		psLogger = configLogger.Sugar()
	} else {
		psLogger = pslog.NewPlanetScaleSugarLogger()
	}

	noglog.SetLogger(&noglog.LoggerFunc{
		DebugfFunc: func(f string, a ...interface{}) { psLogger.Debugf(f, a...) },
		InfofFunc:  func(f string, a ...interface{}) { psLogger.Infof(f, a...) },
		WarnfFunc:  func(f string, a ...interface{}) { psLogger.Warnf(f, a...) },
		ErrorfFunc: func(f string, a ...interface{}) { psLogger.Errorf(f, a...) },
	})

	log.Flush = noglog.Flush
	log.Info = noglog.Info
	log.Infof = noglog.Infof
	log.InfoDepth = noglog.InfoDepth
	log.Warning = noglog.Warning
	log.Warningf = noglog.Warningf
	log.WarningDepth = noglog.WarningDepth
	log.Error = noglog.Error
	log.Errorf = noglog.Errorf
	log.ErrorDepth = noglog.ErrorDepth
	log.Exit = noglog.Exit
	log.Exitf = noglog.Exitf
	log.ExitDepth = noglog.ExitDepth
	log.Fatal = noglog.Fatal
	log.Fatalf = noglog.Fatalf
	log.FatalDepth = noglog.FatalDepth

	return
}
