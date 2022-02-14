package vreplication

import (
	"sort"
	"strings"
	"testing"
)

func TestShardSorter(t *testing.T) {
	shardsArray := []string{
		"c0-c180,f0-,80-c0,40-80,c180-c90,-40,c90-f0",
		"80-,-80",
		"-80,80-",
		"80-a0,fc-,f0-fc,40-80,c0-f0,a0-c0,-40",
		"c0-,60-c0,-60",
		"-60,60-c0,c0-",
		"0-1,55-66,1-11,66-,11-45,45-55",
	}
	sortedShardsArray := []string{
		"-40,40-80,80-c0,c0-c180,c180-c90,c90-f0,f0-",
		"-80,80-",
		"-80,80-",
		"-40,40-80,80-a0,a0-c0,c0-f0,f0-fc,fc-",
		"-60,60-c0,c0-",
		"-60,60-c0,c0-",
		"0-1,1-11,11-45,45-55,55-66,66-",
	}
	for i, shards := range shardsArray {
		arr := strings.Split(shards, ",")
		sort.Sort(ShardSorter(arr))
		newShards := strings.Join(arr, ",")
		if sortedShardsArray[i] != newShards {
			t.Errorf("Shards sorted incorrectly, want %s, got %s", sortedShardsArray[i], newShards)
		}
	}
}
