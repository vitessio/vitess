## Map Shard for Value Tool

### Overview

The `map-shard-for-value` tool maps a given value to a specific shard. This tool helps in determining
which shard a particular value belongs to, based on the vindex algorithm and shard ranges.

### Features
- 

- Allows specifying the vindex type (e.g., `hash`, `xxhash`).
- Allows specifying the shard list of (for uniformly distributed shard ranges) the total number of shards to generate.
- Designed as a _filter_: Reads input values from `stdin` and outputs the corresponding shard information, so it can be
  used to map values from a file or another program.

### Usage

```sh
make build
```

```sh
echo "1\n-1\n99" | ./map-shard-for-value --total_shards=4 --vindex=xxhash
value,keyspaceID,shard
1,d46405367612b4b7,c0-
-1,d8e2a6a7c8c7623d,c0-
99,200533312244abca,-40

echo "1\n-1\n99" | ./map-shard-for-value --vindex=hash --shards="-80,80-"
value,keyspaceID,shard
1,166b40b44aba4bd6,-80
-1,355550b2150e2451,-80
99,2c40ad56f4593c47,-80
```

#### Flags

- `--vindex`: Specifies the name of the vindex to use (e.g., `hash`, `xxhash`) (default `xxhash`)

One (and only one) of these is required:

- `--shards`: Comma-separated list of shard ranges
- `--total_shards`: Total number of shards, only if shards are uniformly distributed

Optional:
- `--value_type`: Type of the value to map, one of int, uint, string (default `int`)

