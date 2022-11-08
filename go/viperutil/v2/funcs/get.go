package funcs

import (
	"strings"

	"github.com/spf13/viper"
)

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
