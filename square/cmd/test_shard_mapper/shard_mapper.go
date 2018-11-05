package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
)

const (
	configFileName = "test/config.json"
	testMappingFilename = "./kochiku_test_mapping.json"
)

type TestShard struct {
	Shard string
	IsDocker bool `json:"is_docker"`
	Flavor string `json:"flavor"`
}

type TestShards struct {
	Shards []TestShard
}

type Config struct {
	Tests map[string]*TestConfig
}

// TestConfig is an entry from the test/config.json file.
type TestConfig struct {
	// Shard is used to split tests among workers.
	Shard string
}

func main() {
	kochikuWorkerPt := flag.Int("worker", -1, "kochiku worker number")
	kochikuWorkerCountPt := flag.Int("worker_count", -1, "kochiku worker count")
	verifyFlag := flag.Bool("verify", false, "verify test mapping")
	flag.Parse()

	if *verifyFlag {
		verify(*kochikuWorkerCountPt)
	} else {
		printShard(*kochikuWorkerPt)
	}
}

func printShard(kochikuWorker int) {
	if kochikuWorker == -1 {
		log.Fatalf("Worker argument is required")
	}

	testShards := loadTestShards()

	if kochikuWorker > len(testShards.Shards) {
		log.Fatalf("Kochiku worker number is not mapped")
	}
	shard := testShards.Shards[kochikuWorker - 1]
	if len(shard.Flavor) == 0 {
		shard.Flavor = "mysql57"
	}
	fmt.Printf("%s %t %s", shard.Shard, shard.IsDocker, shard.Flavor)
}

func verify(kochikuWorkerCount int) {
	if kochikuWorkerCount == -1 {
		log.Fatalf("Worker count is required")
	}

	kochikuTestShards := loadTestShards()

	if kochikuWorkerCount != len(kochikuTestShards.Shards) {
		log.Fatalf("The kochiku worker count (%d) should equal the number of test shards (%d)", kochikuWorkerCount, len(kochikuTestShards.Shards))
	}

	tests := loadTests()
	shards := make(map[string]string)
	for _, test := range tests {
		if len(test.Shard) != 0 {
			shards[test.Shard] = test.Shard
		}
	}

	if len(shards) != len(kochikuTestShards.Shards) {
		log.Fatalf("The number of test shards (%d) should be the same as the number of kochiku shards (%d)", len(shards), len(kochikuTestShards.Shards))
	}

	for _, kochikuShard := range kochikuTestShards.Shards {
		if _, ok := shards[kochikuShard.Shard]; !ok {
			log.Fatalf("kochiku shard %d is not a test shard", kochikuShard.Shard)
		}
	}
}

func loadTestShards() TestShards {
	var testShards TestShards
	byteValue, err := ioutil.ReadFile(testMappingFilename)
	if err != nil {
		log.Fatalf("Cannot open kochiku mapping file: %v", err)
	}

	if err = json.Unmarshal(byteValue, &testShards); err != nil {
		log.Fatalf("Cannot parse kochiku mapping file: %v", err)
	}
	return testShards
}

func loadTests() []TestConfig {
	configData, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Fatalf("Can't read config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Can't parse config file: %v", err)
	}

	tests := make([]TestConfig, 0, len(config.Tests))
	for  _, value := range config.Tests {
		tests = append(tests, *value)
	}
	return tests
}