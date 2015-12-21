#!/bin/bash

# This is an example script that stops guestbook.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping guestbook replicationcontroller..."
$KUBECTL stop replicationcontroller guestbook

echo "Deleting guestbook service..."
$KUBECTL delete service guestbook
