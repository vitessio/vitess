#!/bin/bash

return_value=1
max_attempts=3
for target in $MAKE_TARGET; do
	attempt=1
	echo "Running: make $target (attempt $attempt/$max_attempts)"
	until /usr/bin/time -f "elapsed: %E CPU: %P Memory: %M kB" make $target; do
		if [ $((++attempt)) -gt $max_attempts ]; then
			echo "ERROR: max attempts reached for: make $target"
			exit 1
		fi
	done
done
