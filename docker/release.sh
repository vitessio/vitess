#!/bin/bash
set -ex

vt_base_version='v15.0.3'
debian_versions='buster  bullseye'
default_debian_version='bullseye'

docker pull --platform linux/amd64 vitess/base:$vt_base_version

for debian_version in $debian_versions
do
  echo "####### Building vitess/vt:$debian_version"

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/k8s:$vt_base_version-$debian_version k8s
  docker tag vitess/k8s:$vt_base_version-$debian_version vitess/k8s:$vt_base_version
  docker push vitess/k8s:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/k8s:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtadmin:$vt_base_version-$debian_version k8s/vtadmin
  docker tag vitess/vtadmin:$vt_base_version-$debian_version vitess/vtadmin:$vt_base_version
  docker push vitess/vtadmin:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtadmin:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtgate:$vt_base_version-$debian_version k8s/vtgate
  docker tag vitess/vtgate:$vt_base_version-$debian_version vitess/vtgate:$vt_base_version
  docker push vitess/vtgate:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtgate:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vttablet:$vt_base_version-$debian_version k8s/vttablet
  docker tag vitess/vttablet:$vt_base_version-$debian_version vitess/vttablet:$vt_base_version
  docker push vitess/vttablet:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vttablet:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/mysqlctld:$vt_base_version-$debian_version k8s/mysqlctld
  docker tag vitess/mysqlctld:$vt_base_version-$debian_version vitess/mysqlctld:$vt_base_version
  docker push vitess/mysqlctld:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/mysqlctld:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/mysqlctl:$vt_base_version-$debian_version k8s/mysqlctl
  docker tag vitess/mysqlctl:$vt_base_version-$debian_version vitess/mysqlctl:$vt_base_version
  docker push vitess/mysqlctl:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/mysqlctl:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctl:$vt_base_version-$debian_version k8s/vtctl
  docker tag vitess/vtctl:$vt_base_version-$debian_version vitess/vtctl:$vt_base_version
  docker push vitess/vtctl:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctl:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctlclient:$vt_base_version-$debian_version k8s/vtctlclient
  docker tag vitess/vtctlclient:$vt_base_version-$debian_version vitess/vtctlclient:$vt_base_version
  docker push vitess/vtctlclient:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctlclient:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctld:$vt_base_version-$debian_version k8s/vtctld
  docker tag vitess/vtctld:$vt_base_version-$debian_version vitess/vtctld:$vt_base_version
  docker push vitess/vtctld:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctld:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/logrotate:$vt_base_version-$debian_version k8s/logrotate
  docker tag vitess/logrotate:$vt_base_version-$debian_version vitess/logrotate:$vt_base_version
  docker push vitess/logrotate:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/logrotate:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/logtail:$vt_base_version-$debian_version k8s/logtail
  docker tag vitess/logtail:$vt_base_version-$debian_version vitess/logtail:$vt_base_version
  docker push vitess/logtail:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/logtail:$vt_base_version; fi
done
