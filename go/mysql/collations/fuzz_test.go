//go:build go1.18

// The fuzzing tests for collations use the new Fuzz implementation in Go 1.18+

package collations

import "testing"

func FuzzUCACollate(f *testing.F) {
	for _, left := range AllTestStrings {
		for _, right := range AllTestStrings {
			f.Add([]byte(left.Content), []byte(right.Content))
		}
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, left, right []byte) {
		_ = coll.Collate(left, right, false)
	})
}

func FuzzUCAWeightStrings(f *testing.F) {
	for _, input := range AllTestStrings {
		f.Add([]byte(input.Content))
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, input []byte) {
		_ = coll.WeightString(nil, input, 0)
	})
}
