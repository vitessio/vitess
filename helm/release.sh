#!/bin/bash

version_tag=1.0.4

docker pull vitess/k8s:latest
docker tag vitess/k8s:latest vitess/k8s:helm-$version_tag
docker push vitess/k8s:helm-$version_tag

docker pull vitess/vtgate:latest
docker tag vitess/vtgate:latest vitess/vtgate:helm-$version_tag
docker push vitess/vtgate:helm-$version_tag

docker pull vitess/vttablet:latest
docker tag vitess/vttablet:latest vitess/vttablet:helm-$version_tag
docker push vitess/vttablet:helm-$version_tag

docker pull vitess/mysqlctld:latest
docker tag vitess/mysqlctld:latest vitess/mysqlctld:helm-$version_tag
docker push vitess/mysqlctld:helm-$version_tag

docker pull vitess/vtctl:latest
docker tag vitess/vtctl:latest vitess/vtctl:helm-$version_tag
docker push vitess/vtctl:helm-$version_tag

docker pull vitess/vtctlclient:latest
docker tag vitess/vtctlclient:latest vitess/vtctlclient:helm-$version_tag
docker push vitess/vtctlclient:helm-$version_tag

docker pull vitess/vtctld:latest
docker tag vitess/vtctld:latest vitess/vtctld:helm-$version_tag
docker push vitess/vtctld:helm-$version_tag

docker pull vitess/vtworker:latest
docker tag vitess/vtworker:latest vitess/vtworker:helm-$version_tag
docker push vitess/vtworker:helm-$version_tag

docker pull vitess/logrotate:latest
docker tag vitess/logrotate:latest vitess/logrotate:helm-$version_tag
docker push vitess/logrotate:helm-$version_tag

docker pull vitess/logtail:latest
docker tag vitess/logtail:latest vitess/logtail:helm-$version_tag
docker push vitess/logtail:helm-$version_tag

docker pull vitess/pmm-client:latest
docker tag vitess/pmm-client:latest vitess/pmm-client:helm-$version_tag
docker push vitess/pmm-client:helm-$version_tag

docker pull vitess/orchestrator:latest
docker tag vitess/orchestrator:latest vitess/orchestrator:helm-$version_tag
docker push vitess/orchestrator:helm-$version_tag
