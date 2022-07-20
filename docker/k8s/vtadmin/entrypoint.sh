#!/bin/bash

set -x

cat /etc/nginx/nginx.conf

nginx -c /etc/nginx/nginx.conf -t

/usr/sbin/nginx -g "daemon off;"