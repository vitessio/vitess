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

package cli

import (
	"context"
	"sync/atomic"

	"vitess.io/vitess/go/vt/log"
)

type mySQLTermHandler struct {
	cancelCtx           context.CancelFunc
	cancelBackgroundCtx context.CancelFunc
	ignoreTerms         atomic.Bool
}

func newMySQLTermHandler(cancelCtx, cancelBackgroundCtx context.CancelFunc) *mySQLTermHandler {
	return &mySQLTermHandler{
		cancelCtx:           cancelCtx,
		cancelBackgroundCtx: cancelBackgroundCtx,
	}
}

func (h *mySQLTermHandler) ignoreTermsFor(fn func() error) error {
	h.ignoreTerms.Store(true)
	defer h.ignoreTerms.Store(false)
	return fn()
}

func (h *mySQLTermHandler) onTerm() {
	if h.ignoreTerms.Load() {
		log.Info("Ignoring expected MySQL termination during clone restart")
		return
	}
	log.Warn("Cancelling vtbackup as MySQL has terminated")
	h.cancelCtx()
	h.cancelBackgroundCtx()
}
