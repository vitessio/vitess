// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

func (wr *Wrangler) ExecuteHook(zkTabletPath string, hook *tm.Hook) (hookResult *tm.HookResult, err error) {
	if strings.Contains(hook.Name, "/") {
		return nil, fmt.Errorf("hook name cannot have a '/' in it")
	}
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return nil, err
	}
	zkReplyPath := path.Join(tm.TabletActionPath(zkTabletPath), path.Base(ti.Path())+"_hook_result.json")
	actionPath, err := wr.ai.ExecuteHook(ti.Path(), zkReplyPath, hook)
	if err != nil {
		return nil, err
	}
	err = wr.ai.WaitForCompletion(actionPath, 10*time.Minute)
	if err != nil {
		return nil, err
	}
	data, _, err := wr.zconn.Get(zkReplyPath)
	if err != nil {
		return nil, err
	}
	hr := new(tm.HookResult)
	if err = json.Unmarshal([]byte(data), hr); err != nil {
		return nil, err
	}
	return hr, nil
}
