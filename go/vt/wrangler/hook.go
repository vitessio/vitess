/*
Copyright 2017 Google Inc.

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
