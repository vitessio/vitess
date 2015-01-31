#!/bin/bash

# This is an example script that stops guestbook.

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping guestbook replicationController..."
$KUBECTL stop replicationController guestbook

echo "Deleting guestbook service..."
$KUBECTL delete service guestbook
