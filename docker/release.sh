#!/bin/bash
set -ex

vt_base_version='v7.0.2'
orchestrator_version='3.2.3'
pmm_client_version='1.17.4'

docker pull vitess/base:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/k8s:$vt_base_version-buster .
docker tag vitess/k8s:$vt_base_version-buster vitess/k8s:$vt_base_version
docker push vitess/k8s:$vt_base_version-buster
docker push vitess/k8s:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtgate:$vt_base_version-buster vtgate
docker tag vitess/vtgate:$vt_base_version-buster vitess/vtgate:$vt_base_version
docker push vitess/vtgate:$vt_base_version-buster
docker push vitess/vtgate:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vttablet:$vt_base_version-buster vttablet
docker tag vitess/vttablet:$vt_base_version-buster vitess/vttablet:$vt_base_version
docker push vitess/vttablet:$vt_base_version-buster
docker push vitess/vttablet:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/mysqlctld:$vt_base_version-buster mysqlctld
docker tag vitess/mysqlctld:$vt_base_version-buster vitess/mysqlctld:$vt_base_version
docker push vitess/mysqlctld:$vt_base_version-buster
docker push vitess/mysqlctld:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/mysqlctl:$vt_base_version-buster mysqlctl
docker tag vitess/mysqlctl:$vt_base_version-buster vitess/mysqlctl:$vt_base_version
docker push vitess/mysqlctl:$vt_base_version-buster
docker push vitess/mysqlctl:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctl:$vt_base_version-buster vtctl
docker tag vitess/vtctl:$vt_base_version-buster vitess/vtctl:$vt_base_version
docker push vitess/vtctl:$vt_base_version-buster
docker push vitess/vtctl:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctlclient:$vt_base_version-buster vtctlclient
docker tag vitess/vtctlclient:$vt_base_version-buster vitess/vtctlclient:$vt_base_version
docker push vitess/vtctlclient:$vt_base_version-buster
docker push vitess/vtctlclient:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctld:$vt_base_version-buster vtctld
docker tag vitess/vtctld:$vt_base_version-buster vitess/vtctld:$vt_base_version
docker push vitess/vtctld:$vt_base_version-buster
docker push vitess/vtctld:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtworker:$vt_base_version-buster vtworker
docker tag vitess/vtworker:$vt_base_version-buster vitess/vtworker:$vt_base_version
docker push vitess/vtworker:$vt_base_version-buster
docker push vitess/vtworker:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/logrotate:$vt_base_version-buster logrotate
docker tag vitess/logrotate:$vt_base_version-buster vitess/logrotate:$vt_base_version
docker push vitess/logrotate:$vt_base_version-buster
docker push vitess/logrotate:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/logtail:$vt_base_version-buster logtail
docker tag vitess/logtail:$vt_base_version-buster vitess/logtail:$vt_base_version
docker push vitess/logtail:$vt_base_version-buster
docker push vitess/logtail:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg PMM_CLIENT_VER=$pmm_client_version -t vitess/pmm-client:v$pmm_client_version-buster pmm-client
docker tag vitess/pmm-client:v$pmm_client_version-buster vitess/pmm-client:v$pmm_client_version
docker push vitess/pmm-client:v$pmm_client_version-buster
docker push vitess/pmm-client:v$pmm_client_version

docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg ORC_VER=$orchestrator_version -t vitess/orchestrator:v$orchestrator_version-buster orchestrator
docker tag vitess/orchestrator:v$orchestrator_version-buster vitess/orchestrator:v$orchestrator_version
docker push vitess/orchestrator:v$orchestrator_version-buster
docker push vitess/orchestrator:v$orchestrator_version
