#!/bin/bash

# This is an example script that uses vttablet-up.sh with extra params
# to create a vttablet deployment with 2 shards.

SHARDS='-80,80-'
UID_BASE=200

source ./vttablet-up.sh
