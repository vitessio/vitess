## Map Shard for Value Tool

### Overview

The `map-shard-for-value` tool maps a given value to a specific shard. This tool helps in determining
which shard a particular value belongs to, based on the vindex algorithm and shard ranges.

### Features

- Allows specifying the vindex type (e.g., `hash`, `xxhash`).
- Allows specifying the total number of shards to generate uniformly distributed shard ranges.
- Designed as a _filter_: Reads input values from `stdin` and outputs the corresponding shard information, so it can be 
  used to map values from a file or another program.

### Usage

```sh
make build
```

```sh
echo "1\n-1\n99" | ./map-shard-for-value --total_shards=4 --vindex=xxhash
echo "1\n-1\n99" | ./map-shard-for-value --vindex=hash --shards="-80,80-"
```

#### Flags

- `--vindex`: Specifies the name of the vindex to use (e.g., `hash`, `xxhash`) (default `xxhash`)

One of these is required:
- `--shards`: Comma-separated list of shard ranges
- `--total_shards`: Total number of shards, only if shards are uniformly distributed

