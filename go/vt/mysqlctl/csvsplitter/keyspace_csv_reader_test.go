package csvsplitter

import (
	"io"
	"os"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/key"
)

type pair struct {
	kid  key.KeyspaceId
	line string
}

func readData(t *testing.T, name string, numberColumn bool) []pair {
	file, err := os.Open(testfiles.Locate(name))
	if err != nil {
		t.Fatalf("Cannot open %v: %v", name, err)
	}
	r := NewKeyspaceCSVReader(file, ',', numberColumn)

	keyspaceIds := make([]pair, 0)

	for {
		kid, line, err := r.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		keyspaceIds = append(keyspaceIds, pair{kid, string(line)})
	}

	return keyspaceIds
}

func checkWanted(t *testing.T, got, expected []pair) {
	if len(got) != len(expected) {
		t.Fatalf("Wrong number of records: expected %v, got %v", len(expected), len(got))
	}

	for i, wanted := range expected {
		if got[i].kid != key.KeyspaceId(wanted.kid) {
			t.Errorf("Wrong keyspace_id: expected %#v, got %#v", wanted.kid, got[i].kid)
		}
		if got[i].line != wanted.line {
			t.Errorf("Wrong line: expected %q got %q", wanted.line, got[i].line)
		}
	}

}

func TestCSVSplitterNumber(t *testing.T) {
	// csvsplitter_mean.csv was generated using "select keyspaced_id,
	// tablename.* into outfile".
	keyspaceIds := readData(t, "csvsplitter_mean.csv", true)

	wantedTable := []pair{
		{key.Uint64Key(1).KeyspaceId(), "\"x\x9c\xf3H\xcd\xc9\xc9W(\xcf/\xcaI\x01\\0\x18\xab\x04=\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",1,\"ala\\\nhas a cat\\\n\",1\n"},
		{key.Uint64Key(2).KeyspaceId(), "\"x\x9c\xf3\xc8\xcfIT\xc8-\xcdK\xc9\a\\0\x13\xfe\x03\xc8\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",2,\"ala\\\ntiene un gato\\\\\\\n\r\\\n\",2\n"},
		{key.Uint64Key(3).KeyspaceId(), "\"x\x9cs\xceL\xccW\xc8\xcd\xcfK\xc9\a\\0\x13\x88\x03\xba\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\"ala\\\nha un gatto\\\\n\\\n\",3\n"},
		{key.Uint64Key(4).KeyspaceId(), "\"x\x9cs\xca\xcf\xcb\xca/-R\xc8\xcd\xcfKI\x05\\0#:\x05\x13\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\",,,ala \\\"\\\n,a un chat\",4\n"},
	}

	checkWanted(t, keyspaceIds, wantedTable)
}

func hexOrDie(t *testing.T, hex string) key.KeyspaceId {
	kid, err := key.HexKeyspaceId(hex).Unhex()
	if err != nil {
		t.Fatalf("Unhex failed: %v", err)
	}
	return kid
}

func TestCSVSplitterHex(t *testing.T) {
	// mean.csvcsvsplitter_mean.csv was generated from
	// csvsplitter_mean.csv and changing the ids into hex
	keyspaceIds := readData(t, "csvsplitter_mean_hex.csv", false)

	wantedTable := []pair{
		{hexOrDie(t, "78fe"), "\"x\x9c\xf3H\xcd\xc9\xc9W(\xcf/\xcaI\x01\\0\x18\xab\x04=\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",1,\"ala\\\nhas a cat\\\n\",1\n"},
		{hexOrDie(t, "34ef"), "\"x\x9c\xf3\xc8\xcfIT\xc8-\xcdK\xc9\a\\0\x13\xfe\x03\xc8\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",2,\"ala\\\ntiene un gato\\\\\\\n\r\\\n\",2\n"},
		{hexOrDie(t, "a4f6"), "\"x\x9cs\xceL\xccW\xc8\xcd\xcfK\xc9\a\\0\x13\x88\x03\xba\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\"ala\\\nha un gatto\\\\n\\\n\",3\n"},
		{hexOrDie(t, "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"), "\"x\x9cs\xca\xcf\xcb\xca/-R\xc8\xcd\xcfKI\x05\\0#:\x05\x13\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\",,,ala \\\"\\\n,a un chat\",4\n"},
	}

	checkWanted(t, keyspaceIds, wantedTable)
}
