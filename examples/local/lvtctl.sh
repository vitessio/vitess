#!/bin/bash

# This is a convenience script to run vtctlclient against the local example.

exec vtctlclient -server localhost:15999 "$@"
