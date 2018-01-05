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

/*
Package topocustomrule implements a topo service backed listener for query rules.
One usage is to allow fast propagation of table blacklists.
*/
package topocustomrule

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	// Commandline flag to specify rule cell and path.
	ruleCell = flag.String("topocustomrule_cell", "global", "topo cell for customrules file.")
	rulePath = flag.String("topocustomrule_path", "", "path for customrules file. Disabled if empty.")
)

// topoCustomRuleSource is topo based custom rule source name
const topoCustomRuleSource string = "TOPO_CUSTOM_RULE"

// sleepDuringTopoFailure is how long to sleep before retrying in case of error.
// (it's a var not a const so the test can change the value).
var sleepDuringTopoFailure = 30 * time.Second

// topoCustomRule is the topo backed implementation.
type topoCustomRule struct {
	// qsc is set at construction time.
	qsc tabletserver.Controller

	// conn is the topo connection. Set at construction time.
	conn topo.Conn

	// filePath is the file to read from.
	filePath string

	// qrs is the current rule set that we read.
	qrs *rules.Rules

	// mu protects the following variables.
	mu sync.Mutex

	// cancel is the function to call to cancel the current watch, if any.
	cancel func()

	// stopped is set when stop() is called. It is a protection for race conditions.
	stopped bool
}

func newTopoCustomRule(qsc tabletserver.Controller, cell, filePath string) (*topoCustomRule, error) {
	conn, err := qsc.TopoServer().ConnForCell(context.Background(), cell)
	if err != nil {
		return nil, err
	}
	return &topoCustomRule{
		qsc:      qsc,
		conn:     conn,
		filePath: filePath,
	}, nil
}

func (cr *topoCustomRule) start() {
	go func() {
		for {
			if err := cr.oneWatch(); err != nil {
				log.Warningf("Background watch of topo custom rule failed: %v", err)
			}

			cr.mu.Lock()
			stopped := cr.stopped
			cr.mu.Unlock()

			if stopped {
				log.Warningf("Topo custom rule was terminated")
				return
			}

			log.Warningf("Sleeping for %v before trying again", sleepDuringTopoFailure)
			time.Sleep(sleepDuringTopoFailure)
		}
	}()
}

func (cr *topoCustomRule) stop() {
	cr.mu.Lock()
	if cr.cancel != nil {
		cr.cancel()
	}
	cr.stopped = true
	cr.mu.Unlock()
}

func (cr *topoCustomRule) apply(wd *topo.WatchData) error {
	qrs := rules.New()
	if err := qrs.UnmarshalJSON(wd.Contents); err != nil {
		return fmt.Errorf("error unmarshaling query rules: %v, original data '%s' version %v", err, wd.Contents, wd.Version)
	}

	if !reflect.DeepEqual(cr.qrs, qrs) {
		cr.qrs = qrs.Copy()
		cr.qsc.SetQueryRules(topoCustomRuleSource, qrs)
		log.Infof("Custom rule version %v fetched from topo and applied to vttablet", wd.Version)
	}

	return nil
}

func (cr *topoCustomRule) oneWatch() error {
	defer func() {
		// Whatever happens, cancel() won't be valid after this function exits.
		cr.mu.Lock()
		cr.cancel = nil
		cr.mu.Unlock()
	}()

	ctx := context.Background()
	current, wdChannel, cancel := cr.conn.Watch(ctx, cr.filePath)
	if current.Err != nil {
		return current.Err
	}

	cr.mu.Lock()
	if cr.stopped {
		// We're not interested in the result any more.
		cr.mu.Unlock()
		cancel()
		for range wdChannel {
		}
		return topo.ErrInterrupted
	}
	cr.cancel = cancel
	cr.mu.Unlock()

	if err := cr.apply(current); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return err
	}

	for wd := range wdChannel {
		if wd.Err != nil {
			// Last error value, we're done.
			// wdChannel will be closed right after
			// this, no need to do anything.
			return wd.Err
		}

		if err := cr.apply(wd); err != nil {
			// Cancel the watch, drain channel.
			cancel()
			for range wdChannel {
			}
			return err
		}

	}

	return fmt.Errorf("watch terminated with no error")
}

// activateTopoCustomRules activates topo dynamic custom rule mechanism.
func activateTopoCustomRules(qsc tabletserver.Controller) {
	if *rulePath != "" {
		qsc.RegisterQueryRuleSource(topoCustomRuleSource)

		cr, err := newTopoCustomRule(qsc, *ruleCell, *rulePath)
		if err != nil {
			log.Fatalf("cannot start TopoCustomRule: %v", err)
		}
		cr.start()

		servenv.OnTerm(cr.stop)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, activateTopoCustomRules)
}
