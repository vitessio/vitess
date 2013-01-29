package csvsplitter

import (
	"io"
	"os"
	"testing"

	"code.google.com/p/vitess/go/vt/key"
)

type pair struct {
	kid  key.KeyspaceId
	line string
}

func TestCSVSplitter(t *testing.T) {
	// mean.csv was generated using "select keyspaced_id,
	// tablename.* into outfile".
	file, err := os.Open("mean.csv")
	if err != nil {
		t.Fatalf("Cannot open mean.csv: %v", err)
	}
	r := NewKeyspaceCSVReader(file, ',')

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

	wantedTable := []pair{
		{key.Uint64Key(1).KeyspaceId(), "\"x\x9c\xf3H\xcd\xc9\xc9W(\xcf/\xcaI\x01\\0\x18\xab\x04=\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",1,\"ala\\\nhas a cat\\\n\",1\n"},
		{key.Uint64Key(2).KeyspaceId(), "\"x\x9c\xf3\xc8\xcfIT\xc8-\xcdK\xc9\a\\0\x13\xfe\x03\xc8\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",2,\"ala\\\ntiene un gato\\\\\\\n\r\\\n\",2\n"},
		{key.Uint64Key(3).KeyspaceId(), "\"x\x9cs\xceL\xccW\xc8\xcd\xcfK\xc9\a\\0\x13\x88\x03\xba\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\"ala\\\nha un gatto\\\\n\\\n\",3\n"},
		{key.Uint64Key(4).KeyspaceId(), "\"x\x9cs\xca\xcf\xcb\xca/-R\xc8\xcd\xcfKI\x05\\0#:\x05\x13\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\",,,ala \\\"\\\n,a un chat\",4\n"},
	}

	for i, wanted := range wantedTable {
		if keyspaceIds[i].kid != key.KeyspaceId(wanted.kid) {
			t.Errorf("Wrong keyspace_id: expected %#v, got %#v", wanted.kid, keyspaceIds[i].kid)
		}
		if keyspaceIds[i].line != wanted.line {
			t.Errorf("Wrong line: expected %q got %q", wanted.line, keyspaceIds[i].line)
		}
	}

	if count := len(keyspaceIds); count != 4 {
		t.Errorf("Wrong number of records: expected 4, got %v", count)
	}

}
