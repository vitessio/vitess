package collations

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestGoldenWeights(t *testing.T) {
	gllGoldenTests, err := filepath.Glob("testdata/wiki_*.gob.gz")
	if err != nil {
		t.Fatal(err)
	}

	for _, goldenPath := range gllGoldenTests {
		golden := &GoldenTest{}
		if err := golden.DecodeFromFile(goldenPath); err != nil {
			t.Fatal(err)
		}

		for _, goldenCase := range golden.Cases {
			t.Run(fmt.Sprintf("%s (%s)", golden.Name, goldenCase.Lang), func(t *testing.T) {
				for coll, expected := range goldenCase.Weights {
					coll := testcollation(t, coll)

					input, err := coll.(CollationUCA).Encoding().EncodeFromUTF8(goldenCase.Text)
					if err != nil {
						t.Fatal(err)
					}

					result := coll.WeightString(nil, input, 0)
					if !bytes.Equal(expected, result) {
						t.Errorf("mismatch for collation=%s\noriginal: %s\ninput:    %#v\nexpected: %v\nactual:   %v",
							coll.Name(), string(goldenCase.Text), input, expected, result)
					}
				}
			})
		}
	}
}
