package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
)

func recursivelyDisableAutoGenTags(root *cobra.Command) {
	commands := []*cobra.Command{root}
	for cmd := commands[0]; len(commands) > 0; cmd, commands = commands[0], commands[1:] {
		cmd.DisableAutoGenTag = true

		commands = append(commands, cmd.Commands()...)
	}
}

func main() {
	var dir string
	cmd := cobra.Command{
		Use: "docgen [-d <dir>]",
		RunE: func(cmd *cobra.Command, args []string) error {
			fi, err := os.Stat(dir)
			switch {
			case errors.Is(err, fs.ErrNotExist):
				if err := os.Mkdir(dir, 0755); err != nil {
					return err
				}
			case err != nil:
				return err
			case !fi.IsDir():
				return fmt.Errorf("%s exists but is not a directory", dir)
			}

			recursivelyDisableAutoGenTags(command.Root)
			return doc.GenMarkdownTreeCustom(command.Root, dir, frontmatterFilePrepender, linkHandler)
		},
	}

	cmd.Flags().StringVarP(&dir, "dir", "d", "doc", "output directory to write documentation")
	_ = cmd.Execute()
}

const frontmatter = `---
title: %s
series: %s
description:
---
`

func frontmatterFilePrepender(filename string) string {
	name := filepath.Base(filename)
	base := strings.TrimSuffix(name, filepath.Ext(name))

	root, cmdName, ok := strings.Cut(base, "_")
	if !ok { // no `_`, so not a subcommand
		cmdName = root
	}

	return fmt.Sprintf(frontmatter, cmdName, root)
}

func linkHandler(filename string) string {
	name := filepath.Base(filename)
	base := strings.TrimSuffix(name, filepath.Ext(name))

	if _, _, ok := strings.Cut(base, "_"); !ok {
		return "../"
	}

	return fmt.Sprintf("./%s/", strings.ToLower(base))
}
