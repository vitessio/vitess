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

import (
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
)

// logRotateMaxSize implements pflag.Value and is used to
// try and provide thread-safe access to glog.MaxSize.
type logRotateMaxSize struct {
	val string
}

func (lrms *logRotateMaxSize) Set(s string) error {
	maxSize, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&glog.MaxSize, maxSize)
	lrms.val = s
	return nil
}

func (lrms *logRotateMaxSize) String() string {
	return lrms.val
}

func (lrms *logRotateMaxSize) Type() string {
	return "uint64"
}
