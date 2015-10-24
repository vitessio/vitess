#!/bin/bash

# This script generates config.sh, which is a site-local config file that is not
# checked into source control.

# Select and configure Backup Storage Implementation.
storage=gcs
read -p "Backup Storage (file, gcs) [gcs]: "
if [ -n "$REPLY" ]; then storage="$REPLY"; fi

case "$storage" in
gcs)
  # Google Cloud Storage
  project=$(gcloud config list project | grep 'project\s*=' | sed -r 's/^.*=\s*(.*)$/\1/')
  read -p "Google Developers Console Project [$project]: "
  if [ -n "$REPLY" ]; then project="$REPLY"; fi
  if [ -z "$project" ]; then
    echo "ERROR: Project name must not be empty."
    exit 1
  fi

  read -p "Google Cloud Storage bucket for Vitess backups: " bucket
  if [ -z "$bucket" ]; then
    echo "ERROR: Bucket name must not be empty."
    exit 1
  fi
  echo
  echo "NOTE: If you haven't already created this bucket, you can do so by running:"
  echo "      gsutil mb gs://$bucket"
  echo

  backup_flags=$(echo -backup_storage_implementation gcs \
                      -gcs_backup_storage_project "'$project'" \
                      -gcs_backup_storage_bucket "'$bucket'")
  ;;
file)
  # Mounted volume (e.g. NFS)
  read -p "Root directory for backups (usually an NFS mount): " file_root
  if [ -z "$file_root" ]; then
    echo "ERROR: Root directory must not be empty."
    exit 1
  fi
  echo
  echo "NOTE: You must add your NFS mount to the vtctld-controller-template"
  echo "      and vttablet-pod-template as described in the Kubernetes docs:"
  echo "      http://kubernetes.io/v1.0/docs/user-guide/volumes.html#nfs"
  echo

  backup_flags=$(echo -backup_storage_implementation file \
                      -file_backup_storage_root "'$file_root'")
  ;;
*)
  echo "ERROR: Unsupported backup storage implementation: $storage"
  exit 1
esac

echo "Saving config.sh..."
echo "backup_flags=\"$backup_flags\"" > config.sh

