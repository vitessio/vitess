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

package vtctl

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/throttler/throttlerclient"
	"vitess.io/vitess/go/vt/wrangler"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

const (
	throttlerGroupName = "Resharding Throttler"
	shortTimeout       = 15 * time.Second
)

// This file contains the commands to control the throttler which is used during
// resharding (vtworker) and by filtered replication (vttablet).

func init() {
	addCommandGroup(throttlerGroupName)

	addCommand(throttlerGroupName, command{
		"ThrottlerMaxRates",
		commandThrottlerMaxRates,
		"-server <vtworker or vttablet>",
		"Returns the current max rate of all active resharding throttlers on the server."})
	addCommand(throttlerGroupName, command{
		"ThrottlerSetMaxRate",
		commandThrottlerSetMaxRate,
		"-server <vtworker or vttablet> <rate>",
		"Sets the max rate for all active resharding throttlers on the server."})

	addCommand(throttlerGroupName, command{
		"GetThrottlerConfiguration",
		commandGetThrottlerConfiguration,
		"-server <vtworker or vttablet> [<throttler name>]",
		"Returns the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be returned."})
	addCommand(throttlerGroupName, command{
		"UpdateThrottlerConfiguration",
		commandUpdateThrottlerConfiguration,
		// Note: <configuration protobuf text> is put in quotes to tell the user
		// that the value must be quoted such that it's one argument only.
		`-server <vtworker or vttablet> [-copy_zero_values] "<configuration protobuf text>" [<throttler name>]`,
		"Updates the configuration of the MaxReplicationLag module. The configuration must be specified as protobuf text. If a field is omitted or has a zero value, it will be ignored unless -copy_zero_values is specified. If no throttler name is specified, all throttlers will be updated."})
	addCommand(throttlerGroupName, command{
		"ResetThrottlerConfiguration",
		commandResetThrottlerConfiguration,
		"-server <vtworker or vttablet> [<throttler name>]",
		"Resets the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be reset."})
}

func commandThrottlerMaxRates(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "vtworker or vttablet to connect to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("the ThrottlerSetMaxRate command does not accept any positional parameters")
	}

	// Connect to the server.
	ctx, cancel := context.WithTimeout(ctx, shortTimeout)
	defer cancel()
	client, err := throttlerclient.New(*server)
	if err != nil {
		return fmt.Errorf("error creating a throttler client for server '%v': %v", *server, err)
	}
	defer client.Close()

	rates, err := client.MaxRates(ctx)
	if err != nil {
		return fmt.Errorf("failed to get the throttler rate from server '%v': %v", *server, err)
	}

	if len(rates) == 0 {
		wr.Logger().Printf("There are no active throttlers on server '%v'.\n", *server)
		return nil
	}

	table := tablewriter.NewWriter(loggerWriter{wr.Logger()})
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"Name", "Rate"})
	for name, rate := range rates {
		rateText := strconv.FormatInt(rate, 10)
		if rate == throttler.MaxRateModuleDisabled {
			rateText = "unlimited"
		}
		table.Append([]string{name, rateText})
	}
	table.Render()
	wr.Logger().Printf("%d active throttler(s) on server '%v'.\n", len(rates), *server)
	return nil
}

func commandThrottlerSetMaxRate(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "vtworker or vttablet to connect to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <rate> argument is required for the ThrottlerSetMaxRate command")
	}
	var rate int64
	if strings.ToLower(subFlags.Arg(0)) == "unlimited" {
		rate = throttler.MaxRateModuleDisabled
	} else {
		var err error
		rate, err = strconv.ParseInt(subFlags.Arg(0), 0, 64)
		if err != nil {
			return fmt.Errorf("failed to parse rate '%v' as integer value: %v", subFlags.Arg(0), err)
		}
	}

	// Connect to the server.
	ctx, cancel := context.WithTimeout(ctx, shortTimeout)
	defer cancel()
	client, err := throttlerclient.New(*server)
	if err != nil {
		return fmt.Errorf("error creating a throttler client for server '%v': %v", *server, err)
	}
	defer client.Close()

	names, err := client.SetMaxRate(ctx, rate)
	if err != nil {
		return fmt.Errorf("failed to set the throttler rate on server '%v': %v", *server, err)
	}

	if len(names) == 0 {
		wr.Logger().Printf("ThrottlerSetMaxRate did nothing because server '%v' has no active throttlers.\n", *server)
		return nil
	}

	printUpdatedThrottlers(wr.Logger(), *server, names)
	return nil
}

func commandGetThrottlerConfiguration(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "vtworker or vttablet to connect to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 1 {
		return fmt.Errorf("the GetThrottlerConfiguration command accepts only <throttler name> as optional positional parameter")
	}

	var throttlerName string
	if subFlags.NArg() == 1 {
		throttlerName = subFlags.Arg(0)
	}

	// Connect to the server.
	ctx, cancel := context.WithTimeout(ctx, shortTimeout)
	defer cancel()
	client, err := throttlerclient.New(*server)
	if err != nil {
		return fmt.Errorf("error creating a throttler client for server '%v': %v", *server, err)
	}
	defer client.Close()

	configurations, err := client.GetConfiguration(ctx, throttlerName)
	if err != nil {
		return fmt.Errorf("failed to get the throttler configuration from server '%v': %v", *server, err)
	}

	if len(configurations) == 0 {
		wr.Logger().Printf("There are no active throttlers on server '%v'.\n", *server)
		return nil
	}

	table := tablewriter.NewWriter(loggerWriter{wr.Logger()})
	table.SetAutoFormatHeaders(false)
	// The full protobuf text will span more than one terminal line. Do not wrap
	// it to make it easy to copy and paste it.
	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Name", "Configuration (protobuf text, fields with a zero value are omitted)"})
	for name, c := range configurations {
		table.Append([]string{name, proto.CompactTextString(c)})
	}
	table.Render()
	wr.Logger().Printf("%d active throttler(s) on server '%v'.\n", len(configurations), *server)
	return nil
}

func commandUpdateThrottlerConfiguration(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "vtworker or vttablet to connect to")
	copyZeroValues := subFlags.Bool("copy_zero_values", false, "If true, fields with zero values will be copied as well")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 2 {
		return fmt.Errorf(`the "<configuration protobuf text>" argument is required for the UpdateThrottlerConfiguration command. The <throttler name> is an optional positional parameter`)
	}

	var throttlerName string
	if subFlags.NArg() == 2 {
		throttlerName = subFlags.Arg(1)
	}

	protoText := subFlags.Arg(0)
	configuration := &throttlerdatapb.Configuration{}
	if err := proto.UnmarshalText(protoText, configuration); err != nil {
		return fmt.Errorf("failed to unmarshal the configuration protobuf text (%v) into a protobuf instance: %v", protoText, err)
	}

	// Connect to the server.
	ctx, cancel := context.WithTimeout(ctx, shortTimeout)
	defer cancel()
	client, err := throttlerclient.New(*server)
	if err != nil {
		return fmt.Errorf("error creating a throttler client for server '%v': %v", *server, err)
	}
	defer client.Close()

	names, err := client.UpdateConfiguration(ctx, throttlerName, configuration, *copyZeroValues)
	if err != nil {
		return fmt.Errorf("failed to update the throttler configuration on server '%v': %v", *server, err)
	}

	if len(names) == 0 {
		wr.Logger().Printf("UpdateThrottlerConfiguration did nothing because server '%v' has no active throttlers.\n", *server)
		return nil
	}

	printUpdatedThrottlers(wr.Logger(), *server, names)
	wr.Logger().Printf("The new configuration will become effective with the next recalculation event.\n")
	return nil
}

func commandResetThrottlerConfiguration(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "vtworker or vttablet to connect to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 1 {
		return fmt.Errorf("the ResetThrottlerConfiguration command accepts only <throttler name> as optional positional parameter")
	}

	var throttlerName string
	if subFlags.NArg() == 1 {
		throttlerName = subFlags.Arg(0)
	}

	// Connect to the server.
	ctx, cancel := context.WithTimeout(ctx, shortTimeout)
	defer cancel()
	client, err := throttlerclient.New(*server)
	if err != nil {
		return fmt.Errorf("error creating a throttler client for server '%v': %v", *server, err)
	}
	defer client.Close()

	names, err := client.ResetConfiguration(ctx, throttlerName)
	if err != nil {
		return fmt.Errorf("failed to get the throttler configuration from server '%v': %v", *server, err)
	}

	if len(names) == 0 {
		wr.Logger().Printf("ResetThrottlerConfiguration did nothing because server '%v' has no active throttlers.\n", *server)
		return nil
	}

	printUpdatedThrottlers(wr.Logger(), *server, names)
	wr.Logger().Printf("The reset initial configuration will become effective with the next recalculation event.\n")
	return nil
}

func printUpdatedThrottlers(logger logutil.Logger, server string, names []string) {
	table := tablewriter.NewWriter(loggerWriter{logger})
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"Name"})
	for _, name := range names {
		table.Append([]string{name})
	}
	table.Render()
	logger.Printf("%d active throttler(s) on server '%v' were updated.\n", len(names), server)
}
