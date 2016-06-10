// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/throttler/throttlerclient"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

// This file contains the commands to control the throttler which is used during
// resharding (vtworker) and by filtered replication (vttablet).

const throttlerGroupName = "Resharding Throttler"
const shortTimeout = 15 * time.Second

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

	table := tablewriter.NewWriter(loggerWriter{wr.Logger()})
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"Name"})
	for _, name := range names {
		table.Append([]string{name})
	}
	table.Render()
	wr.Logger().Printf("%d active throttler(s) on server '%v' were updated.\n", len(names), *server)
	return nil
}
