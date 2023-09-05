/*
Copyright 2022 The Vitess Authors.

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

// Package docgen provides common logic for generating markdown doctrees from
// a root cobra.Command for the vitessio/website repository.
//
// Example usage:
//
//	package main
//
//	import (
//		"github.com/spf13/cobra"
//
//		"vitess.io/vitess/go/cmd/internal/docgen"
//		vtctldclient "vitess.io/vitess/go/cmd/vtctldclient/command"
//	)
//
//	func main() {
//		cmd := &cobra.Command{
//			RunE: func(cmd *cobra.Command, args []string) error {
//				dir := cmd.Flags().Arg(0)
//				return docgen.GenerateMarkdownTree(vtctldclient.Root, dir)
//			}
//			Args: cobra.ExactArgs(1),
//		}
//
//		cmd.Execute()
//	}
package docgen

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// GenerateMarkdownTree generates a markdown doctree for the root cobra.Command
// written to `dir`. The root command is also renamed to _index.md to remain
// compatible with the vitessio/website content structure expectations.
func GenerateMarkdownTree(cmd *cobra.Command, dir string) error {
	switch fi, err := os.Stat(dir); {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	case err != nil:
		return err
	case !fi.IsDir():
		return fmt.Errorf("%s exists but is not a directory", dir)
	}

	recursivelyDisableAutoGenTags(cmd)
	if err := doc.GenMarkdownTreeCustom(cmd, dir, frontmatterFilePrepender, linkHandler); err != nil {
		return err
	}

	rootDocPath := filepath.Join(dir, cmd.Name()+".md")
	indexDocPath := filepath.Join(dir, "_index.md")
	if err := os.Rename(rootDocPath, indexDocPath); err != nil {
		return fmt.Errorf("failed to index doc (generated at %s) into proper position (%s): %w", rootDocPath, indexDocPath, err)
	}

	if err := restructure(dir, dir, cmd.Name(), cmd.Commands()); err != nil {
		return err
	}

	return nil
}

/*
_index.md (aka vtctldclient.md)
vtctldclient_AddCellInfo.md
vtctldclient_movetables.md
vtctldclient_movetables_show.md

becomes

_index.md
vtctldclient_AddCellInfo.md
vtctldclient_movetables/
	_index.md
	vtctldclient_movetables_show.md
*/

func restructure(rootDir string, dir string, name string, commands []*cobra.Command) error {
	for _, cmd := range commands {
		fullCmdFilename := strings.Join([]string{name, cmd.Name()}, "_")

		children := cmd.Commands()

		switch {
		case len(children) > 0:
			// Command (top-level or not) with children.
			// 1. Set up a directory for its children.
			// 2. Move its doc into that dir as "_index.md"
			// 3. Restructure its children.
			cmdDir := filepath.Join(dir, fullCmdFilename)
			if err := os.MkdirAll(cmdDir, 0755); err != nil {
				return fmt.Errorf("failed to create subdir for %s: %w", fullCmdFilename, err)
			}

			if err := os.Rename(filepath.Join(rootDir, fullCmdFilename+".md"), filepath.Join(cmdDir, "_index.md")); err != nil {
				return fmt.Errorf("failed to move index doc for command %s with children: %w", fullCmdFilename, err)
			}

			if err := restructure(rootDir, cmdDir, fullCmdFilename, children); err != nil {
				return fmt.Errorf("failed to restructure child commands for %s: %w", fullCmdFilename, err)
			}
		case rootDir != dir:
			// Sub-command without children.
			// 1. Move its doc into the directory for its parent, name unchanged.
			if cmd.Name() == "help" {
				// all commands with children have their own "help" subcommand,
				// which we do not generate docs for
				continue
			}

			oldName := filepath.Join(rootDir, fullCmdFilename+".md")
			newName := filepath.Join(dir, fullCmdFilename+".md")

			if err := os.Rename(oldName, newName); err != nil {
				return fmt.Errorf("failed to move child command %s to its parent's dir: %w", fullCmdFilename, err)
			}

			sed := newParentLinkSedCommand(name, newName)
			if out, err := sed.CombinedOutput(); err != nil {
				return fmt.Errorf("failed to rewrite links to parent command in child %s: %w (extra: %s)", newName, err, out)
			}
		default:
			// Top-level command without children. Nothing to restructure.
			continue
		}
	}

	return nil
}

func newParentLinkSedCommand(parent string, file string) *exec.Cmd {
	return exec.Command("sed", "-i", "", "-e", fmt.Sprintf("s:(./%s/):(../):i", parent), file)
}

func recursivelyDisableAutoGenTags(root *cobra.Command) {
	commands := []*cobra.Command{root}
	for cmd := commands[0]; len(commands) > 0; cmd, commands = commands[0], commands[1:] {
		cmd.DisableAutoGenTag = true

		commands = append(commands, cmd.Commands()...)
	}
}

const frontmatter = `---
title: %s
series: %s
---
`

func frontmatterFilePrepender(filename string) string {
	name := filepath.Base(filename)
	base := strings.TrimSuffix(name, filepath.Ext(name))

	root, cmdName, ok := strings.Cut(base, "_")
	if !ok { // no `_`, so not a subcommand
		cmdName = root
	}

	cmdName = strings.ReplaceAll(cmdName, "_", " ")

	return fmt.Sprintf(frontmatter, cmdName, root)
}

func linkHandler(filename string) string {
	base := filepath.Base(filename)
	name := strings.TrimSuffix(base, filepath.Ext(base))

	_, _, ok := strings.Cut(name, "_")
	if !ok {
		return "../"
	}

	return fmt.Sprintf("./%s/", strings.ToLower(name))
}
