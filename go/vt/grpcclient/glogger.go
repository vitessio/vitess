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

package grpcclient

import (
	"fmt"

	"google.golang.org/grpc/grpclog"

	"vitess.io/vitess/go/vt/log"
)

// grpc doesn't return underlying errors. So, we have
// to rely on logs to know the root cause if a request
// fails. Using grpc's glogger is too spammy. So, we
// perform more selective logging using these functions.
func init() {
	grpclog.SetLoggerV2(&glogger{})
}

type glogger struct{}

func (g *glogger) Info(args ...any) {
}

func (g *glogger) Infoln(args ...any) {
}

func (g *glogger) Infof(format string, args ...any) {
}

func (g *glogger) Warning(args ...any) {
	log.WarningDepth(2, args...)
}

func (g *glogger) Warningln(args ...any) {
	log.WarningDepth(2, fmt.Sprintln(args...))
}

func (g *glogger) Warningf(format string, args ...any) {
	log.WarningDepth(2, fmt.Sprintf(format, args...))
}

func (g *glogger) Error(args ...any) {
	log.ErrorDepth(2, args...)
}

func (g *glogger) Errorln(args ...any) {
	log.ErrorDepth(2, fmt.Sprintln(args...))
}

func (g *glogger) Errorf(format string, args ...any) {
	log.ErrorDepth(2, fmt.Sprintf(format, args...))
}

func (g *glogger) Fatal(args ...any) {
	log.FatalDepth(2, args...)
}

func (g *glogger) Fatalln(args ...any) {
	log.FatalDepth(2, fmt.Sprintln(args...))
}

func (g *glogger) Fatalf(format string, args ...any) {
	log.FatalDepth(2, fmt.Sprintf(format, args...))
}

func (g *glogger) V(l int) bool {
	return bool(log.V(log.Level(l)))
}
