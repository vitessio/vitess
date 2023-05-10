/*
Copyright 2023 The Vitess Authors.

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

package logutil

import (
	noglog "github.com/slok/noglog"
	"go.uber.org/zap"

	"vitess.io/vitess/go/vt/log"
)

type VTSLogger zap.SugaredLogger

// SetVTStructureLogger in-place noglog replacement with Zap's logger.
func SetVTStructureLogger(conf *zap.Config) (vtSLogger *zap.SugaredLogger, err error) {
	var l *zap.Logger

	// Use the passed configuration instead of the default configuration
	if conf == nil {
		defaultProdConf := zap.NewProductionConfig()
		conf = &defaultProdConf
	}

	// Build configuration and generate a sugared logger
	l, err = conf.Build()
	vtSLogger = l.Sugar()

	noglog.SetLogger(&noglog.LoggerFunc{
		DebugfFunc: func(f string, a ...interface{}) { vtSLogger.Debugf(f, a...) },
		InfofFunc:  func(f string, a ...interface{}) { vtSLogger.Infof(f, a...) },
		WarnfFunc:  func(f string, a ...interface{}) { vtSLogger.Warnf(f, a...) },
		ErrorfFunc: func(f string, a ...interface{}) { vtSLogger.Errorf(f, a...) },
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
