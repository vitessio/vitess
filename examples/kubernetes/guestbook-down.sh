#!/bin/bash

# This is an example script that stops guestbook.

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping guestbook replicationcontroller..."
$KUBECTL $KUBECTL_OPTIONS delete replicationcontroller guestbook

echo "Deleting guestbook service..."
$KUBECTL $KUBECTL_OPTIONS delete service guestbook
