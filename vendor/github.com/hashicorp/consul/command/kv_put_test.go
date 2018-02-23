package command

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/mitchellh/cli"
)

func TestKVPutCommand_implements(t *testing.T) {
	var _ cli.Command = &KVPutCommand{}
}

func TestKVPutCommand_noTabs(t *testing.T) {
	assertNoTabs(t, new(KVPutCommand))
}

func TestKVPutCommand_Validation(t *testing.T) {
	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	cases := map[string]struct {
		args   []string
		output string
	}{
		"-acquire without -session": {
			[]string{"-acquire", "foo"},
			"Missing -session",
		},
		"-release without -session": {
			[]string{"-release", "foo"},
			"Missing -session",
		},
		"-cas no -modify-index": {
			[]string{"-cas", "foo"},
			"Must specify -modify-index",
		},
		"no key": {
			[]string{},
			"Missing KEY argument",
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

func TestKVPutCommand_Run(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"foo", "bar",
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data.Value, []byte("bar")) {
		t.Errorf("bad: %#v", data.Value)
	}
}

func TestKVPutCommand_File(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	f, err := ioutil.TempFile("", "kv-put-command-file")
	if err != nil {
		t.Fatalf("err: %#v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("bar"); err != nil {
		t.Fatalf("err: %#v", err)
	}

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"foo", "@" + f.Name(),
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data.Value, []byte("bar")) {
		t.Errorf("bad: %#v", data.Value)
	}
}

func TestKVPutCommand_FileNoExist(t *testing.T) {
	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	args := []string{
		"foo", "@/nope/definitely/not-a-real-file.txt",
	}

	code := c.Run(args)
	if code == 0 {
		t.Fatal("bad: expected error")
	}

	output := ui.ErrorWriter.String()
	if !strings.Contains(output, "Failed to read file") {
		t.Errorf("bad: %#v", output)
	}
}

func TestKVPutCommand_Stdin(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	stdinR, stdinW := io.Pipe()

	ui := new(cli.MockUi)
	c := &KVPutCommand{
		Ui:        ui,
		testStdin: stdinR,
	}

	go func() {
		stdinW.Write([]byte("bar"))
		stdinW.Close()
	}()

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"foo", "-",
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data.Value, []byte("bar")) {
		t.Errorf("bad: %#v", data.Value)
	}
}

func TestKVPutCommand_NegativeVal(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"foo", "-2",
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data.Value, []byte("-2")) {
		t.Errorf("bad: %#v", data.Value)
	}
}

func TestKVPutCommand_Flags(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"-flags", "12345",
		"foo",
	}

	code := c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if data.Flags != 12345 {
		t.Errorf("bad: %#v", data.Flags)
	}
}

func TestKVPutCommand_CAS(t *testing.T) {
	srv, client := testAgentWithAPIClient(t)
	defer srv.Shutdown()
	waitForLeader(t, srv.httpAddr)

	// Create the initial pair so it has a ModifyIndex.
	pair := &api.KVPair{
		Key:   "foo",
		Value: []byte("bar"),
	}
	if _, err := client.KV().Put(pair, nil); err != nil {
		t.Fatalf("err: %#v", err)
	}

	ui := new(cli.MockUi)
	c := &KVPutCommand{Ui: ui}

	args := []string{
		"-http-addr=" + srv.httpAddr,
		"-cas",
		"-modify-index", "123",
		"foo", "a",
	}

	code := c.Run(args)
	if code == 0 {
		t.Fatalf("bad: expected error")
	}

	data, _, err := client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Reset buffers
	ui.OutputWriter.Reset()
	ui.ErrorWriter.Reset()

	args = []string{
		"-http-addr=" + srv.httpAddr,
		"-cas",
		"-modify-index", strconv.FormatUint(data.ModifyIndex, 10),
		"foo", "a",
	}

	code = c.Run(args)
	if code != 0 {
		t.Fatalf("bad: %d. %#v", code, ui.ErrorWriter.String())
	}

	data, _, err = client.KV().Get("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data.Value, []byte("a")) {
		t.Errorf("bad: %#v", data.Value)
	}
}
