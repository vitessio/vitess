// +build !race

package race

// Enabled is set to true in the build if the race detector is enabled.
// This is useful to skip tests when the race detector is on.
// This is the same approach as in: https://golang.org/src/internal/race/
const Enabled = false
