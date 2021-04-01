/*
Copyright 2021 The Vitess Authors.

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
Package command contains the commands used by vtctldclient. It is intended only
for use in vtctldclient's main package and entrypoint. The rest of this
documentation is intended for maintainers.

Commands are grouped into files by the types of resources they interact with (
e.g. GetTablet, CreateTablet, DeleteTablet, GetTablets) or by what they do (e.g.
PlannedReparentShard, EmergencyReparentShard, InitShardPrimary). Please add the
command to the appropriate existing file, alphabetically, or create a new
grouping if one does not exist.

The root command lives in root.go, and commands must attach themselves to this
during an init function in order to be reachable from the CLI. root.go also
contains the global variables available to any subcommand that are managed by
the root command's pre- and post-run functions. Commands must not attempt to
manage these, as that may conflict with Root's post-run cleanup actions. All
commands should, at a minimum, use the commandCtx rather than creating their own
context.Background to start, as it contains root tracing spans that would be
lost.

Commands should not keep their logic in an anonymous function on the
cobra.Command struct, but instead in a separate function that is assigned to
RunE. Commands should strive to keep declaration, function definition, and flag
initialization located as closely together as possible, to make the code easier
to follow and understand (the global variables declared near Root are the
exception here, not the rule). Commands should also prevent individual flag
names from polluting the package namespace.

A good pattern we have found is to do the following:
	package command

	// (imports ...)

	var (
		CreateTablet = &cobra.Command{
			Use: "CreateTablet [options] --keyspace=<keyspace> --shard=<shard-range> <tablet-alias> <tablet-type>",
			Args: cobra.ExactArgs(2),
			RunE: commandCreateTablet,
		}
		GetTablet = &cobra.Command{
			Use: "GetTablet <tablet-alias>",
			Args: cobra.ExactArgs(1),
			RunE: commandGetTablet,
		}
	)

	var createTabletOptions = struct {
		Opt1 string
		Opt2 bool
		Keyspace string
		Shard string
	}{}

	func commandCreateTablet(cmd *cobra.Command, args []string) error {
		aliasStr := cmd.Flags().Args(0)
		tabletTypeStr := cmd.Flags().Args(1)

		// do stuff with:
		// - client
		// - commandCtx
		// - createTabletOptions
		// - aliasStr
		// - tabletTypeStr

		return nil
	}

	// GetTablet takes no flags, so it needs no anonymous struct to store them
	func commandGetTablet(cmd *cobra.Command, args []string) error {
		aliasStr := cmd.Flags().Arg(0)

		// do stuff with:
		// - client
		// - commandCtx
		// - aliasStr

		return nil
	}

	// finally, hook up all the commands in this file to Root, and add any flags
	// to each of those commands

	func init() {
		CreateTablet.Flags().StringVar(&createTabletOptions.Opt1, "opt1", "default", "help")
		CreateTablet.Flags().BoolVar(&createTabletOptions.Opt2, "opt2", false, "help")
		CreateTablet.Flags().StringVarP(&createTabletOptions.Keyspace, "keyspace", "k", "keyspace of tablet")
		CreateTablet.MarkFlagRequired("keyspace")
		CreateTablet.Flags().StringVarP(&createTabletOptions.Shard, "shard", "s", "shard range of tablet")
		CreateTablet.MarkFlagRequired("shard")
		Root.AddCommand(CreateTablet)

		Root.AddCommand(GetTablet)
	}

A note on RunE and SilenceUsage:

We prefer using RunE over Run for the entrypoint to our subcommands, because it
allows us return errors back up to the vtctldclient main function and do error
handling, logging, and exit-code management once, in one place, rather than on a
per-command basis. However, cobra treats errors returned from a command's RunE
as usage errors, and therefore will print the command's full usage text to
stderr when RunE returns non-nil, in addition to propagating that error back up
to the result of the root command's Execute() method. This is decidedly not what
we want. There is no plan to address this in cobra v1. [1]

The suggested workaround for this issue is to set SilenceUsage: true, either on
the root command or on every subcommand individually. This also does not work
for vtctldclient, because not every flag can be parsed during pflag.Parse time,
and for certain flags (mutually exclusive options, optional flags that require
other flags to be set with them, etc) we do additional parsing and validation of
flags in an individual subcommand. We want errors during this phase to be
treated as usage errors, so setting SilenceUsage=true before this point would
not cause usage text to be printed for us.

So, for us, we want to individually set cmd.SilenceUsage = true at *particular
points* in each command, dependending on whether that command needs to do
an additional parse & validation pass. In most cases, the command does not need
to post-validate its options, and can set cmd.SilencUsage = true as their first
line. We feel, though, that a line that reads "SilenceUsage = true" to be
potentially confusing in how it reads. A maintainer without sufficient context
may read this and say "Silence usage? We don't want that" and remove the lines,
so we provide a wrapper function that communicates intent, cli.FinishedParsing,
that each subcommand should call when they have transitioned from the parsing &
validation phase of their entrypoint to the actual logic.

[1]: https://github.com/spf13/cobra/issues/340
*/
package command
