package funcs

import (
	"strings"

	"github.com/spf13/viper"
)

// GetPath returns a GetFunc that expands a slice of strings into individual
// paths based on standard POSIX shell $PATH separator parsing.
func GetPath(v *viper.Viper) func(key string) []string {
	return func(key string) (paths []string) {
		for _, val := range v.GetStringSlice(key) {
			if val != "" {
				for _, path := range strings.Split(val, ":") {
					paths = append(paths, path)
				}
			}
		}

		return paths
	}
}
