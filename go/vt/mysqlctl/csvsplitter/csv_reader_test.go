package csvsplitter

import (
	"io"
	"os"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func readLines(t *testing.T, name string) []string {
	file, err := os.Open(testfiles.Locate(name))
	if err != nil {
		t.Fatalf("Cannot open %v: %v", name, err)
	}
	r := NewCSVReader(file, ',')

	lines := make([]string, 0)

	for {
		line, err := r.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		lines = append(lines, string(line))
	}

	return lines
}

func checkWantedLines(t *testing.T, got, expected []string) {
	if len(got) != len(expected) {
		t.Fatalf("Wrong number of records: expected %v, got %v", len(expected), len(got))
	}

	for i, wanted := range expected {
		if got[i] != wanted {
			t.Errorf("Wrong line: expected %q got %q", wanted, got[i])
		}
	}

}

func TestCSVReader1(t *testing.T) {
	// csvsplitter_mean.csv was generated using "select keyspaced_id,
	// tablename.* into outfile".
	lines := readLines(t, "csvsplitter_mean.csv")

	wantedTable := []string{
		"1,\"x\x9c\xf3H\xcd\xc9\xc9W(\xcf/\xcaI\x01\\0\x18\xab\x04=\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",1,\"ala\\\nhas a cat\\\n\",1\n",
		"2,\"x\x9c\xf3\xc8\xcfIT\xc8-\xcdK\xc9\a\\0\x13\xfe\x03\xc8\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",2,\"ala\\\ntiene un gato\\\\\\\n\r\\\n\",2\n",
		"3,\"x\x9cs\xceL\xccW\xc8\xcd\xcfK\xc9\a\\0\x13\x88\x03\xba\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\"ala\\\nha un gatto\\\\n\\\n\",3\n",
		"4,\"x\x9cs\xca\xcf\xcb\xca/-R\xc8\xcd\xcfKI\x05\\0#:\x05\x13\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\",,,ala \\\"\\\n,a un chat\",4\n",
	}

	checkWantedLines(t, lines, wantedTable)
}

func TestCSVReader2(t *testing.T) {
	// mean.csvcsvsplitter_mean.csv was generated from
	// csvsplitter_mean.csv and changing the ids into hex
	lines := readLines(t, "csvsplitter_mean_hex.csv")

	wantedTable := []string{
		"\"78fe\",\"x\x9c\xf3H\xcd\xc9\xc9W(\xcf/\xcaI\x01\\0\x18\xab\x04=\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",1,\"ala\\\nhas a cat\\\n\",1\n",
		"\"34ef\",\"x\x9c\xf3\xc8\xcfIT\xc8-\xcdK\xc9\a\\0\x13\xfe\x03\xc8\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",2,\"ala\\\ntiene un gato\\\\\\\n\r\\\n\",2\n",
		"\"A4F6\",\"x\x9cs\xceL\xccW\xc8\xcd\xcfK\xc9\a\\0\x13\x88\x03\xba\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\"ala\\\nha un gatto\\\\n\\\n\",3\n",
		"\"1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef\",\"x\x9cs\xca\xcf\xcb\xca/-R\xc8\xcd\xcfKI\x05\\0#:\x05\x13\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\",3,\",,,ala \\\"\\\n,a un chat\",4\n",
	}

	checkWantedLines(t, lines, wantedTable)
}
