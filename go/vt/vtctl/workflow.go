/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctl

import (
	"encoding/json"
	"flag"
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/workflow/resharding"
	"github.com/youtube/vitess/go/vt/workflow/topovalidator"
	"github.com/youtube/vitess/go/vt/wrangler"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// This file contains the workflows command group for vtctl.

const workflowsGroupName = "Workflows"

var (
	// WorkflowManager contains our manager. It needs to be set or else all
	// commands will be disabled.
	WorkflowManager *workflow.Manager
)

func init() {
	// Register the various workflow factories to enable the WorkflowShow
	// introspection commands to function
	topovalidator.Register()
	resharding.Register()

	addCommandGroup(workflowsGroupName)

	addCommand(workflowsGroupName, command{
		"WorkflowCreate",
		commandWorkflowCreate,
		"[-skip_start] <factoryName> [parameters...]",
		"Creates the workflow with the provided parameters. The workflow is also started, unless -skip_start is specified."})
	addCommand(workflowsGroupName, command{
		"WorkflowStart",
		commandWorkflowStart,
		"<uuid>",
		"Starts the workflow."})
	addCommand(workflowsGroupName, command{
		"WorkflowStop",
		commandWorkflowStop,
		"<uuid>",
		"Stops the workflow."})
	addCommand(workflowsGroupName, command{
		"WorkflowDelete",
		commandWorkflowDelete,
		"<uuid>",
		"Deletes the finished or not started workflow."})
	addCommand(workflowsGroupName, command{
		"WorkflowWait",
		commandWorkflowWait,
		"<uuid>",
		"Waits for the workflow to finish."})
	addCommand(workflowsGroupName, command{
		"WorkflowShow",
		commandWorkflowShow,
		"<uuid>",
		"Displays a JSON representation of the workflow."})
	addCommand(workflowsGroupName, command{
		"WorkflowNames",
		commandWorkflowNames,
		"",
		"Displays a list of active workflows."})
	addCommand(workflowsGroupName, command{
		"WorkflowTree",
		commandWorkflowTree,
		"",
		"Displays a JSON representation of the workflow tree."})
	addCommand(workflowsGroupName, command{
		"WorkflowAction",
		commandWorkflowAction,
		"<path> <name>",
		"Sends the provided action name on the specified path."})
}

func commandWorkflowCreate(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	skipStart := subFlags.Bool("skip_start", false, "If set, the workflow will not be started.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 1 {
		return fmt.Errorf("the <factoryName> argument is required for the WorkflowCreate command")
	}
	factoryName := subFlags.Arg(0)

	uuid, err := WorkflowManager.Create(ctx, factoryName, subFlags.Args()[1:])
	if err != nil {
		return err
	}
	wr.Logger().Printf("uuid: %v\n", uuid)

	if !*skipStart {
		return WorkflowManager.Start(ctx, uuid)
	}
	return nil
}

func commandWorkflowStart(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <uuid> argument is required for the WorkflowStart command")
	}
	uuid := subFlags.Arg(0)
	return WorkflowManager.Start(ctx, uuid)
}

func commandWorkflowStop(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <uuid> argument is required for the WorkflowStop command")
	}
	uuid := subFlags.Arg(0)
	return WorkflowManager.Stop(ctx, uuid)
}

func commandWorkflowDelete(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <uuid> argument is required for the WorkflowDelete command")
	}
	uuid := subFlags.Arg(0)
	return WorkflowManager.Delete(ctx, uuid)
}

func commandWorkflowWait(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <uuid> argument is required for the WorkflowWait command")
	}
	uuid := subFlags.Arg(0)
	return WorkflowManager.Wait(ctx, uuid)
}

type workflowInfo struct {
	UUID        string
	FactoryName string
	Name        string
	State       workflowpb.WorkflowState
	Error       string
	StartTime   int64
	EndTime     int64
	Data        workflow.Data
}

func commandWorkflowShow(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <uuid> argument is required for the WorkflowWait command")
	}
	uuid := subFlags.Arg(0)
	wi, wd, err := WorkflowManager.GetWorkflowData(ctx, uuid)
	if err != nil {
		return err
	}

	info := workflowInfo{
		UUID:        wi.Uuid,
		FactoryName: wi.FactoryName,
		Name:        wi.Name,
		State:       wi.State,
		Error:       wi.Error,
		StartTime:   wi.StartTime,
		EndTime:     wi.EndTime,
		Data:        wd,
	}
	json, err := json.MarshalIndent(info, "", "    ")
	if err != nil {
		return err
	}

	wr.Logger().Printf("%s\n", string(json))
	return nil
}

func commandWorkflowNames(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("the WorkflowNames command takes no parameter")
	}

	uuids, err := WorkflowManager.GetWorkflowNames(ctx)
	if err != nil {
		return err
	}
	for _, uuid := range uuids {
		wr.Logger().Printf("%s\n", uuid)
	}
	return nil
}

func commandWorkflowTree(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("the WorkflowTree command takes no parameter")
	}

	tree, err := WorkflowManager.NodeManager().GetFullTree()
	if err != nil {
		return err
	}
	wr.Logger().Printf("%v\n", string(tree))
	return nil
}

func commandWorkflowAction(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if WorkflowManager == nil {
		return fmt.Errorf("no workflow.Manager registered")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <path> and <name> arguments are required for the WorkflowAction command")
	}
	ap := &workflow.ActionParameters{
		Path: subFlags.Arg(0),
		Name: subFlags.Arg(1),
	}

	return WorkflowManager.NodeManager().Action(ctx, ap)
}
