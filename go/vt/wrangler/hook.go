// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	hk "github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/topo"
)

func (wr *Wrangler) ExecuteHook(tabletAlias topo.TabletAlias, hook *hk.Hook) (hookResult *hk.HookResult, err error) {
	if strings.Contains(hook.Name, "/") {
		return nil, fmt.Errorf("hook name cannot have a '/' in it")
	}
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.ExecuteTabletInfoHook(ti, hook)
}

func (wr *Wrangler) ExecuteTabletInfoHook(ti *topo.TabletInfo, hook *hk.Hook) (hookResult *hk.HookResult, err error) {

	actionPath, err := wr.ai.ExecuteHook(ti.Alias(), hook)
	if err != nil {
		return nil, err
	}

	var hr interface{}
	if hr, err = wr.ai.WaitForCompletionReply(actionPath, 10*time.Minute); err != nil {
		return nil, err
	}
	return hr.(*hk.HookResult), nil
}

// Execute a hook and returns an error only if the hook failed, not if
// the hook doesn't exist.
func (wr *Wrangler) ExecuteOptionalTabletInfoHook(ti *topo.TabletInfo, hook *hk.Hook) (err error) {
	hr, err := wr.ExecuteTabletInfoHook(ti, hook)
	if err != nil {
		return err
	}

	if hr.ExitStatus == hk.HOOK_DOES_NOT_EXIST {
		log.Infof("Hook %v doesn't exist on tablet %v", hook.Name, ti.Alias())
		return nil
	}

	if hr.ExitStatus != hk.HOOK_SUCCESS {
		return fmt.Errorf("Hook %v failed(%v): %v", hook.Name, hr.ExitStatus, hr.Stderr)
	}

	return nil
}
