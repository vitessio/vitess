#!/bin/bash
set -ex

vt_base_version='v13.0.0'
debian_versions='buster  bullseye'
default_debian_version='bullseye'

docker pull vitess/base:$vt_base_version

for debian_version in $debian_versions
do
  echo "####### Building vitess/vt:$debian_version"

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/k8s:$vt_base_version-$debian_version .
  docker tag vitess/k8s:$vt_base_version-$debian_version vitess/k8s:$vt_base_version
  docker push vitess/k8s:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/k8s:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtgate:$vt_base_version-$debian_version vtgate
  docker tag vitess/vtgate:$vt_base_version-$debian_version vitess/vtgate:$vt_base_version
  docker push vitess/vtgate:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtgate:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vttablet:$vt_base_version-$debian_version vttablet
  docker tag vitess/vttablet:$vt_base_version-$debian_version vitess/vttablet:$vt_base_version
  docker push vitess/vttablet:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vttablet:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/mysqlctld:$vt_base_version-$debian_version mysqlctld
  docker tag vitess/mysqlctld:$vt_base_version-$debian_version vitess/mysqlctld:$vt_base_version
  docker push vitess/mysqlctld:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/mysqlctld:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/mysqlctl:$vt_base_version-$debian_version mysqlctl
  docker tag vitess/mysqlctl:$vt_base_version-$debian_version vitess/mysqlctl:$vt_base_version
  docker push vitess/mysqlctl:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/mysqlctl:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctl:$vt_base_version-$debian_version vtctl
  docker tag vitess/vtctl:$vt_base_version-$debian_version vitess/vtctl:$vt_base_version
  docker push vitess/vtctl:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctl:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctlclient:$vt_base_version-$debian_version vtctlclient
  docker tag vitess/vtctlclient:$vt_base_version-$debian_version vitess/vtctlclient:$vt_base_version
  docker push vitess/vtctlclient:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctlclient:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtctld:$vt_base_version-$debian_version vtctld
  docker tag vitess/vtctld:$vt_base_version-$debian_version vitess/vtctld:$vt_base_version
  docker push vitess/vtctld:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtctld:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/vtworker:$vt_base_version-$debian_version vtworker
  docker tag vitess/vtworker:$vt_base_version-$debian_version vitess/vtworker:$vt_base_version
  docker push vitess/vtworker:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/vtworker:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/logrotate:$vt_base_version-$debian_version logrotate
  docker tag vitess/logrotate:$vt_base_version-$debian_version vitess/logrotate:$vt_base_version
  docker push vitess/logrotate:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/logrotate:$vt_base_version; fi

  docker build --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t vitess/logtail:$vt_base_version-$debian_version logtail
  docker tag vitess/logtail:$vt_base_version-$debian_version vitess/logtail:$vt_base_version
  docker push vitess/logtail:$vt_base_version-$debian_version
  if [[ $debian_version == $default_debian_version ]]; then docker push vitess/logtail:$vt_base_version; fi
done
