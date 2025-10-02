/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	flag "github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

/*
 * This tool reads a list of values from stdin and prints the
 * corresponding keyspace ID and shard for each value. It uses the given vindex
 * and shard ranges to determine the shard. The vindex is expected to be a
 * single-column vindex. The shard ranges are specified as a comma-separated
 * list of key ranges, example "-80,80-".
 * If you have uniformly distributed shards, you can specify the total number
 * of shards using the -total_shards flag, and the tool will generate the shard ranges
 * using the same logic as the Vitess operator does (using the key.GenerateShardRanges() function).
 *
 * Example usage:
 * echo "1\n2\n3" | go run shard-from-id.go -vindex=hash -shards=-80,80-
 *
 * Currently tested only for integer values and hash/xxhash vindexes.
 */

func mapShard(allShards []*topodata.ShardReference, ksid key.DestinationKeyspaceID) (string, error) {
	foundShard := ""
	addShard := func(shard string) error {
		foundShard = shard
		return nil
	}
	if err := ksid.Resolve(allShards, addShard); err != nil {
		return "", fmt.Errorf("failed to resolve keyspace ID: %v:: %s", ksid.String(), err)
	}

	if foundShard == "" {
		return "", fmt.Errorf("no shard found for keyspace ID: %v", ksid)
	}
	return foundShard, nil
}

func selectShard(vindex vindexes.Vindex, value sqltypes.Value, allShards []*topodata.ShardReference) (string, key.DestinationKeyspaceID, error) {
	ctx := context.Background()

	destinations, err := vindexes.Map(ctx, vindex, nil, [][]sqltypes.Value{{value}})
	if err != nil {
		return "", nil, fmt.Errorf("failed to map value to keyspace ID: %w", err)
	}

	if len(destinations) != 1 {
		return "", nil, fmt.Errorf("unexpected number of destinations: %d", len(destinations))
	}

	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok {
		return "", nil, fmt.Errorf("unexpected destination type: %T", destinations[0])
	}

	foundShard, err := mapShard(allShards, ksid)
	if err != nil {
		return "", nil, fmt.Errorf("failed to map shard, original value %v, keyspace id %s: %w", value, ksid, err)
	}
	return foundShard, ksid, nil
}

func getValue(valueStr, valueType string) (sqltypes.Value, error) {
	var value sqltypes.Value

	switch valueType {
	case "int":
		valueInt, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return value, fmt.Errorf("failed to parse int value: %w", err)
		}
		value = sqltypes.NewInt64(valueInt)
	case "uint":
		valueUint, err := strconv.ParseUint(valueStr, 10, 64)
		if err != nil {
			return value, fmt.Errorf("failed to parse uint value: %w", err)
		}
		value = sqltypes.NewUint64(valueUint)
	case "string":
		value = sqltypes.NewVarChar(valueStr)
	default:
		return value, fmt.Errorf("unsupported value type: %s", valueType)
	}

	return value, nil
}

func getShardMap(shardsCSV *string) []*topodata.ShardReference {
	var allShards []*topodata.ShardReference

	for _, shard := range strings.Split(*shardsCSV, ",") {
		_, keyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			log.Fatalf("invalid shard range: %s", shard)
		}
		allShards = append(allShards, &topodata.ShardReference{
			Name:     shard,
			KeyRange: keyRange,
		})
	}
	return allShards
}

type output struct {
	Value      string
	KeyspaceID string
	Shard      string
}

func processValues(scanner *bufio.Scanner, shardsCSV *string, vindexName string, valueType string) ([]output, error) {
	allShards := getShardMap(shardsCSV)

	vindex, err := vindexes.CreateVindex(vindexName, vindexName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create vindex: %v", err)
	}
	var outputs []output
	for scanner.Scan() {
		valueStr := scanner.Text()
		if valueStr == "" {
			continue
		}
		value, err := getValue(valueStr, valueType)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for: %v, value_type %s:: %v", valueStr, valueType, err)
		}
		shard, ksid, err := selectShard(vindex, value, allShards)
		if err != nil {
			// ignore errors so that we can go ahead with the computation for other values
			continue
		}
		outputs = append(outputs, output{Value: valueStr, KeyspaceID: hex.EncodeToString(ksid), Shard: shard})
	}
	return outputs, nil
}

func printOutput(outputs []output) {
	fmt.Println("value,keyspaceID,shard")
	for _, output := range outputs {
		fmt.Printf("%s,%s,%s\n", output.Value, output.KeyspaceID, output.Shard)
	}
}

func main() {
	// Explicitly configuring the logger since it was flaky in displaying logs locally without this.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("LOG: ")

	vindexName := flag.String("vindex", "xxhash", "name of the vindex")
	shardsCSV := flag.String("shards", "", "comma-separated list of shard ranges")
	totalShards := flag.Int("total_shards", 0, "total number of uniformly distributed shards")
	valueType := flag.String("value_type", "int", "type of the value (int, uint, or string)")
	flag.Parse()

	if *totalShards > 0 {
		if *shardsCSV != "" {
			log.Fatalf("cannot specify both total_shards and shards")
		}
		shardArr, err := key.GenerateShardRanges(*totalShards, 0)
		if err != nil {
			log.Fatalf("failed to generate shard ranges: %v", err)
		}
		*shardsCSV = strings.Join(shardArr, ",")
	}
	if *shardsCSV == "" {
		log.Fatal("shards or total_shards must be specified")
	}
	scanner := bufio.NewScanner(os.Stdin)
	outputs, err := processValues(scanner, shardsCSV, *vindexName, *valueType)
	if err != nil {
		log.Fatalf("failed to process values: %v", err)
	}
	printOutput(outputs)
}
