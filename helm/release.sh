#!/bin/bash

vt_base_version=v7.0.1
orchestrator_version=3.2.3
pmm_client_version=1.17.4

docker pull vitess/base:$vt_base_version

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/k8s:$vt_base_version-buster .
docker push vitess/k8s:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtgate:$vt_base_version-buster vtgate
docker push vitess/vtgate:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vttablet:$vt_base_version-buster vttablet
docker push vitess/vttablet:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/mysqlctld:$vt_base_version-buster mysqlctld
docker push vitess/mysqlctld:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctl:$vt_base_version-buster vtctl
docker push vitess/vtctl:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctlclient:$vt_base_version-buster vtctlclient
docker push vitess/vtctlclient:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtctld:$vt_base_version-buster vtctld
docker push vitess/vtctld:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/vtworker:$vt_base_version-buster vtworker
docker push vitess/vtworker:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/logrotate:$vt_base_version-buster logrotate
docker push vitess/logrotate:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version -t vitess/logtail:$vt_base_version-buster logtail
docker push vitess/logtail:$vt_base_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg PMM_CLIENT_VER=$pmm_client_version -t vitess/pmm-client:v$pmm_client_version-buster pmm-client
docker push vitess/pmm-client:v$pmm_client_version-buster

docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg PMM_CLIENT_VER=$pmm_client_version -t vitess/orchestrator:v$orchestrator_version-buster orchestrator
docker push vitess/orchestrator:v$orchestrator_version-buster
