// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"

	hk "github.com/youtube/vitess/go/vt/hook"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ExecuteHook will run the hook on the tablet
func (wr *Wrangler) ExecuteHook(ctx context.Context, tabletAlias *topodatapb.TabletAlias, hook *hk.Hook) (hookResult *hk.HookResult, err error) {
	if strings.Contains(hook.Name, "/") {
		return nil, fmt.Errorf("hook name cannot have a '/' in it")
	}
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.ExecuteTabletHook(ctx, ti.Tablet, hook)
}

// ExecuteTabletHook will run the hook on the provided tablet.
func (wr *Wrangler) ExecuteTabletHook(ctx context.Context, tablet *topodatapb.Tablet, hook *hk.Hook) (hookResult *hk.HookResult, err error) {
	return wr.tmc.ExecuteHook(ctx, tablet, hook)
}
