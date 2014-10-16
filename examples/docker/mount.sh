#!/bin/bash

# This script is an example of how to mount the exported volumes of a running
# container into a new one so you can look at, e.g., log and data files.

# Usage: mount.sh <container>

docker run -ti --rm --volumes-from $1 -w /vt/vtdataroot vitess/base /bin/bash
