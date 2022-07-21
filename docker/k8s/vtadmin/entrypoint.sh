#!/bin/sh

set -x

nginx -c /etc/nginx/nginx.conf -t

/usr/sbin/nginx -g "daemon off;"