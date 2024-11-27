package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

/*
 * This is a simple tool that reads a list of values from stdin and prints the
 * corresponding keyspace ID and shard for each value. It uses the given vindex
 * and shard ranges to determine the shard. The vindex is expected to be a
 * single-column vindex. The shard ranges are specified as a comma-separated
 * list of key ranges, example "-80,80-".
 * If you have uniformly distributed shards, you can specify the total number
 * of shards using the -total_shards flag, and the tool will generate the shard ranges.
 *
 * Example usage:
 * echo "1\n2\n3" | go run shard-from-id.go -vindex=hash -shards=-80,80-
 *
 * Currently tested only for integer values and hash/xxhash vindexes.
 */

func mapShard(shards map[string]*topodata.KeyRange, ksid key.DestinationKeyspaceID) (string, error) {
	foundShard := ""
	addShard := func(shard string) error {
		foundShard = shard
		return nil
	}
	allShards := make([]*topodata.ShardReference, 0, len(shards))
	for shard, keyRange := range shards {
		allShards = append(allShards, &topodata.ShardReference{
			Name:     shard,
			KeyRange: keyRange,
		})
	}
	if err := ksid.Resolve(allShards, addShard); err != nil {
		return "", fmt.Errorf("failed to resolve keyspace ID: %v:: %s", ksid.String(), err)
	}

	if foundShard == "" {
		return "", fmt.Errorf("no shard found for keyspace ID: %v", ksid)
	}
	return foundShard, nil
}

func selectShard(vindex vindexes.Vindex, value sqltypes.Value, shards map[string]*topodata.KeyRange) (string, key.DestinationKeyspaceID, error) {
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

	foundShard, err := mapShard(shards, ksid)
	if err != nil {
		return "", nil, fmt.Errorf("failed to map shard: %w", err)
	}
	return foundShard, ksid, nil
}

func getValue(valueStr string) (sqltypes.Value, int64, error) {
	var value sqltypes.Value
	valueInt, err := strconv.ParseInt(valueStr, 10, 64)
	if err == nil {
		value = sqltypes.NewInt64(int64(valueInt))
	} else {
		valueUint, err := strconv.ParseUint(valueStr, 10, 64)
		if err == nil {
			value = sqltypes.NewUint64(valueUint)
		} else {
			value = sqltypes.NewVarChar(valueStr)
		}
	}
	return value, valueInt, err
}

func getShardMap(shardsCSV *string) map[string]*topodata.KeyRange {
	shards := make(map[string]*topodata.KeyRange)
	var err error
	for _, shard := range strings.Split(*shardsCSV, ",") {
		parts := strings.Split(shard, "-")
		var start, end []byte
		if len(parts) > 0 && parts[0] != "" {
			start, err = hex.DecodeString(parts[0])
			if err != nil {
				log.Fatalf("failed to decode shard start: %v", err)
			}
		}
		if len(parts) > 1 && parts[1] != "" {
			end, err = hex.DecodeString(parts[1])
			if err != nil {
				log.Fatalf("failed to decode shard end: %v", err)
			}
		}
		shards[shard] = &topodata.KeyRange{Start: start, End: end}
	}
	return shards
}

func main() {
	vindexName := flag.String("vindex", "xxhash", "name of the vindex")
	shardsCSV := flag.String("shards", "", "comma-separated list of shard ranges")
	totalShards := flag.Int("total_shards", 0, "total number of uniformly distributed shards")
	flag.Parse()

	if *totalShards > 0 {
		if *shardsCSV != "" {
			log.Fatalf("cannot specify both total_shards and shards")
		}
		shardArr, err := key.GenerateShardRanges(*totalShards)
		if err != nil {
			log.Fatalf("failed to generate shard ranges: %v", err)
		}
		*shardsCSV = strings.Join(shardArr, ",")
	}
	if *shardsCSV == "" {
		log.Fatal("shards or total_shards must be specified")
	}

	shards := getShardMap(shardsCSV)

	vindex, err := vindexes.CreateVindex(*vindexName, *vindexName, nil)
	if err != nil {
		log.Fatalf("failed to create vindex: %v", err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	first := true
	for scanner.Scan() {
		valueStr := scanner.Text()
		if valueStr == "" {
			continue
		}
		value, _, err := getValue(valueStr)

		shard, ksid, err := selectShard(vindex, value, shards)
		if err != nil {
			// ignore errors so that we can go ahead with the computation for other values
			continue
		}

		if first {
			// print header
			fmt.Println("value,keyspaceID,shard")
			first = false
		}

		ksidStr := hex.EncodeToString([]byte(ksid))
		fmt.Printf("%s,%s,%s\n", valueStr, ksidStr, shard)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading from stdin: %v", err)
	}
}
