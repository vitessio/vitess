#!/bin/bash

# This is an example script that stops guestbook.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

echo "Deleting pods created by guestbook replicationController..."
kubecfg.sh stop guestbook

echo "Deleting guestbook replicationController..."
kubecfg.sh delete replicationControllers/guestbook

echo "Deleting guestbook service..."
kubecfg.sh delete services/guestbook
