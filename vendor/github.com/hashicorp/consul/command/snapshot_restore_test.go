package command

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/mitchellh/cli"
)

func TestSnapshotRestoreCommand_implements(t *testing.T) {
	var _ cli.Command = &SnapshotRestoreCommand{}
}

func TestSnapshotRestoreCommand_noTabs(t *testing.T) {
	assertNoTabs(t, new(SnapshotRestoreCommand))
}

func TestSnapshotRestoreCommand_Validation(t *testing.T) {
	ui := new(cli.MockUi)
	c := &SnapshotRestoreCommand{Ui: ui}

	cases := map[string]struct {
		args   []string
		output string
	}{
		"no file": {
			[]string{},
			"Missing FILE argument",
		},
		"extra args": {
			[]string{"foo", "bar", "baz"},
			"Too many arguments",
		},
	}

	for name, tc := range cases {
		// Ensure our buffer is always clear
		if ui.ErrorWriter != nil {
			ui.ErrorWriter.Reset()
		}
		if ui.OutputWriter != nil {
			ui.OutputWriter.Reset()
		}

		code := c.Run(tc.args)
		if code == 0 {
			t.Errorf("%s: expected non-zero exit", name)
		}

		output := ui.ErrorWriter.String()
		if !strings.Contains(output, tc.output) {
			t.Errorf("%s: expected %q to contain %q", name, output, tc.output)
		}
	}
}

func TestSnapshotRestoreCommand_Run(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	ui := new(cli.MockUi)
	c := &SnapshotSaveCommand{Ui: ui}

	dir, err := ioutil.TempDir("", "snapshot")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(dir)

	file := path.Join(dir, "backup.tgz")
	args := []string{
		"-http-addr=" + srv.httpAddr,
		file,
	}

	f, err := os.Create(file)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	snap, _, err := client.Snapshot().Save(nil)
	if err != nil {
		f.Close()
		t.Fatalf("err: %v", err)
	}
	if _, err := io.Copy(f, snap); err != nil {
		f.Close()
		t.Fatalf("err: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}
}
