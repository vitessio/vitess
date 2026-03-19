#!/usr/bin/bash
# MicroCeph S3 setup for CI. Single-node cluster with RGW.
# Adapted from canonical/microceph-action with Vitess CI fixes.

set -eux

usage() {
  echo "Usage: $0 [-c <snap-channel>] [-a <access-key>] [-s <secret-key>] [-b <bucket-name>] [-z <disk-size>]" 1>&2
  exit 1
}

CHANNEL=squid/stable
ACCESS_KEY=vitess
SECRET_KEY=vitess-secret-key
BUCKET_NAME=vitess-test
DISK_SIZE=4G

while getopts ":c:a:s:b:z:" o; do
  case "${o}" in
    c) CHANNEL=${OPTARG} ;;
    a) ACCESS_KEY=${OPTARG} ;;
    s) SECRET_KEY=${OPTARG} ;;
    b) BUCKET_NAME=${OPTARG} ;;
    z) DISK_SIZE=${OPTARG} ;;
    *) usage ;;
  esac
done

check_ceph_ok_or_exit() {
  for i in $(seq 1 10); do
    if sudo microceph.ceph status | grep -q HEALTH_OK; then
      echo "Cluster healthy."
      return 0
    fi
    sudo microceph.ceph status
    if [ "$i" -eq 10 ]; then
      echo "::error::Cluster did not reach HEALTH_OK"
      sudo microceph.ceph health detail
      exit 1
    fi
    sleep 10
  done
}

sudo apt-get -qq -y update
sudo apt-get -qq -y install s3cmd

# Clear libeatmydata LD_PRELOAD — breaks snap confinement on GitHub runners
sudo truncate -s 0 /etc/ld.so.preload
sudo systemctl restart snapd
sudo snap wait system seed.loaded

if ls microceph-snap-cache/microceph_*.snap 1>/dev/null 2>&1; then
  sudo snap ack microceph-snap-cache/microceph_*.assert
  sudo snap install microceph-snap-cache/microceph_*.snap --dangerous
else
  sudo snap install microceph --channel="${CHANNEL}"
fi

sudo snap connect microceph:hardware-observe
sudo snap connect microceph:block-devices
sudo snap restart microceph.daemon

# OSD processes hit "ulimit: open files: cannot modify limit" on GitHub runners.
# Set LimitNOFILE so the daemon inherits a high fd limit.
sudo mkdir -p /etc/systemd/system/snap.microceph.osd.service.d
sudo mkdir -p /etc/systemd/system.conf.d
printf '[Service]\nLimitNOFILE=65536\n' | sudo tee /etc/systemd/system/snap.microceph.osd.service.d/override.conf
printf '[Manager]\nDefaultLimitNOFILE=65536\n' | sudo tee /etc/systemd/system.conf.d/limit-nofile.conf
sudo systemctl daemon-reload

sudo microceph cluster bootstrap
sleep 30

sudo microceph.ceph config set "mon.$(hostname)" mon_data_avail_warn 6

# Single OSD avoids ulimit failure when adding 2nd/3rd OSD (snapctl restart fails).
# Pool replication 1 is required for HEALTH_OK with one OSD.
sudo microceph disk add loop,"${DISK_SIZE}",1
sudo microceph pool set-rf "*" 1

check_ceph_ok_or_exit

sudo microceph enable rgw
sleep 15

for i in $(seq 1 30); do
  if sudo ss -ltnp | grep -q radosgw; then break; fi
  echo "Waiting for RGW socket ($i/30)..."
  if [ "$i" -eq 30 ]; then
    echo "::error::RGW did not bind within 60s"
    sudo microceph.ceph status || true
    sudo snap logs microceph.rgw --last=50 || true
    exit 1
  fi
  sleep 2
done

RGW_PORT=$(sudo ss -ltnp \
  | awk '/radosgw/{ match($4, /:([0-9]+)$/, a); if (a[1]) print a[1] }' \
  | head -1)

if [ -z "${RGW_PORT}" ]; then
  echo "::error::Could not parse RGW port"
  exit 1
fi
echo "RGW listening on port ${RGW_PORT}"

# Use ACCESS_KEY as uid so S3 credential matches
sudo microceph.radosgw-admin user create \
  --uid="${ACCESS_KEY}" --display-name="Vitess Test User"
sudo microceph.radosgw-admin key create \
  --uid="${ACCESS_KEY}" --key-type=s3 \
  --access-key="${ACCESS_KEY}" --secret-key="${SECRET_KEY}"

s3cmd --host "localhost:${RGW_PORT}" \
  --host-bucket="localhost:${RGW_PORT}/%(bucket)" \
  --access_key="${ACCESS_KEY}" \
  --secret_key="${SECRET_KEY}" \
  --no-ssl \
  mb "s3://${BUCKET_NAME}"

check_ceph_ok_or_exit

# Write sourceable config (canonical-style output)
OUTPUT="$(pwd)/microceph.source"
{
  echo "AWS_ACCESS_KEY_ID=${ACCESS_KEY}"
  echo "AWS_SECRET_ACCESS_KEY=${SECRET_KEY}"
  echo "AWS_ENDPOINT=http://localhost:${RGW_PORT}"
  echo "AWS_BUCKET=${BUCKET_NAME}"
  echo "AWS_REGION=us-east-1"
} > "${OUTPUT}"

# Export to GITHUB_ENV so Vitess tests receive vars without sourcing
if [ -n "${GITHUB_ENV:-}" ]; then
  while IFS= read -r line; do
    echo "$line" >> "$GITHUB_ENV"
  done < "${OUTPUT}"
fi

echo "::notice::MicroCeph S3 ready — http://localhost:${RGW_PORT} | bucket: ${BUCKET_NAME}"
