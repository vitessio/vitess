#!/bin/bash

if ! /usr/bin/getent group vitess >/dev/null ; then
   groupadd -r vitess
fi

if ! /usr/bin/getent passwd vitess >/dev/null ; then
    useradd -r -g vitess vitess
fi
