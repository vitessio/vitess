#!/bin/bash

# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###############################################################################
### rotate.sh                                                               ###
###                                                                         ###
### Rotates mysqld logfiles. By default it rotates files matching wildcard: ###
###                                                                         ###
###     /vtdataroot/tabletdata/*.log                                        ###
###                                                                         ###
### This behavior may be changed via environmenet variables or cli args.    ###
###                                                                         ###
### For example, this invocation will change the wilcard expression:        ###
###                                                                         ###
###     rotate.sh --wildcard-path /some/other/location/*.log                ###
###                                                                         ###
### Rather than relying on the wildcard expression, it is recommended to    ###
### instead disable this behavior and specify the individual log files to   ###
### be rotated. The invnocation below uses the provided mysql socket to     ###
### discover the location of the general log, and disables rotation of      ###
### files matching a wildcard:                                              ###
###                                                                         ###
###     rotate.sh \                                                         ###
###         --general-log-discover-path \                                   ###
###         --mysql-socket /vt/mysql.sock \                                 ###
###         --wildcard-path /dev/zero                                       ###
###                                                                         ###
###############################################################################

###
### DEBUGGING
###
### By default, rotate.sh prints what is doing through set -x, and exits on the
### first error with set -e. These behaviors can be disabled respectively via:
###
###     NO_DEBUG=1
###     NO_EXIT_ON_ERROR=1
###

if [ -z "$NO_DEBUG" ]; then
  set -x
fi

if [ -z "$NO_EXIT_ON_ERROR" ]; then
  set -e
fi

###
### CONSTANTS
###
### Set some constants to be used throughout the program.
###

LOGROTATE=/usr/sbin/logrotate
LOGROTATEROOT="${LOGROTATEROOT:-/vt}"
LOGROTATE_CONF="$LOGROTATEROOT/logrotate.conf"
LOGROTATE_CONF_TPL="$LOGROTATEROOT/logrotate.conf.tpl"
LOGROTATE_STATUS=$LOGROTATEROOT/logrotate.status
LOG_PATH_UNDEFINED="$LOGROTATEROOT/undefined.log"

###
### ENVIRONMENT VARIABLES
###
### These environment variables are used to configure the behavior of the
### program. They are also used to store the values of cli args.
###
### Values set by cli args take precedence over values set by environment
### variables.
###

# error_log settings
AUDIT_LOG_DISCOVER_PATH=${AUDIT_LOG_DISCOVER_PATH:-0}
AUDIT_LOG_PATH=${AUDIT_LOG_PATH:-}
AUDIT_LOG_ROTATE=${AUDIT_LOG_ROTATE:-}
AUDIT_LOG_SIZE=${AUDIT_LOG_SIZE:-}

# create files as user/group
CREATE_GROUP=${GROUP:-vitess}
CREATE_GROUP=${CREATE_GROUP:-$GROUP}
CREATE_USER=${USER:-vitess}
CREATE_USER=${CREATE_USER:-$USER}

# default logrotate settings
DEFAULT_ROTATE=${DEFAULT_ROTATE:-1}
DEFAULT_SIZE=${DEFAULT_SIZE:-100M}

# error_log settings
ERROR_LOG_DISCOVER_PATH=${ERROR_LOG_DISCOVER_PATH:-0}
ERROR_LOG_PATH=${ERROR_LOG_PATH:-}
ERROR_LOG_ROTATE=${ERROR_LOG_ROTATE:-}
ERROR_LOG_SIZE=${ERROR_LOG_SIZE:-}

# general_log settings
GENERAL_LOG_DISCOVER_PATH=${GENERAL_LOG_DISCOVER_PATH:-0}
GENERAL_LOG_PATH=${GENERAL_LOG_PATH:-}
GENERAL_LOG_ROTATE=${GENERAL_LOG_ROTATE:-}
GENERAL_LOG_SIZE=${GENERAL_LOG_SIZE:-}

# interval (in seconds) between logrotate invocations
ROTATE_INTERVAL=${ROTATE_INTERVAL:-3600}

# mysql socket and user used to flush logs and discover log paths
MYSQL_HOST=${MYSQL_HOST:-}
MYSQL_SOCKET=${MYSQL_SOCKET:-/vtdataroot/tabletdata/mysql.sock}
MYSQL_USER=${MYSQL_USER:-root}

# print logrotate conf
PRINT_LOGROTATE_CONF=${PRINT_LOGROTATE_CONF:-0}

# slow_query_log settings
SLOW_QUERY_LOG_DISCOVER_PATH=${SLOW_QUERY_LOG_DISCOVER_PATH:-}
SLOW_QUERY_LOG_PATH=${SLOW_QUERY_LOG_PATH:-}
SLOW_QUERY_LOG_ROTATE=${SLOW_QUERY_LOG_ROTATE:-}
SLOW_QUERY_LOG_SIZE=${SLOW_QUERY_LOG_SIZE:-}

# wildcard settings
WILDCARD_PATH=${WILDCARD_PATH:-/vtdataroot/tabletdata/*.log}
WILDCARD_ROTATE=${WILDCARD_ROTATE:-}
WILDCARD_SIZE=${WILDCARD_SIZE:-}

###
### FUNCTIONS
###

log_error() {
  >&2 echo "$@"
}

log_exit() {
  log_error "$@"
  exit 1
}

# logrotate_conf_generate
#
# Exports a bunch of environment variables, and uses envsubst to generate a
# logrotate configuration file. By dynamically generating a configuration file,
# logrotate.sh will react when the location of a mysql log file to change (e.g.
# through general_log_file).
logrotate_conf_generate() {
  vars=""
  if ! vars=$(logrotate_conf_vars_prepare); then
    log_error "failed to prepare logrotate conf vars"
    return 1
  fi

  # shellcheck disable=SC2068
  for kv in $vars; do
    # shellcheck disable=SC2163
    if ! export "$kv"; then
      log_error "failed to export log rotate conf var $kv"
      return 1
    fi
  done

  envsubst < "$LOGROTATE_CONF_TPL" > "$LOGROTATE_CONF"
}

# logrotate_conf_vars_prepare
#
# Prepare environment variables which may dynamically change between logrotate
# invocations. Echo a bunch of lines of the form k=v. These lines are then used
# to export variables to envsubst.
logrotate_conf_vars_prepare() {
  for section in audit_log error_log general_log slow_query_log; do
    if ! logrotate_conf_vars_prepare_$section; then
      log_error "failed to prepare logrotate conf vars for: $section"
      return 1
    fi
  done
}

logrotate_conf_vars_prepare_audit_log() {
  if [ "$AUDIT_LOG_DISCOVER_PATH" = "1" ]; then
    if path=$(mysql_global_variable_get audit_log_file) && [ -n "$path" ]; then
      echo AUDIT_LOG_PATH="$path"
    else
      log_error "failed to discover audit_log path"
      return 1
    fi
  fi
}

logrotate_conf_vars_prepare_error_log() {
  if [ "$ERROR_LOG_DISCOVER_PATH" = "1" ]; then
    if path=$(mysql_global_variable_get log_error) && [ -n "$path" ]; then
      echo ERROR_LOG_PATH="$path"
    else
      log_error "failed to discover error_log path"
      return 1
    fi
  fi
}

logrotate_conf_vars_prepare_general_log() {
  if [ "$GENERAL_LOG_DISCOVER_PATH" = "1" ]; then
    if path=$(mysql_global_variable_get general_log_file) && [ -n "$path" ]; then
      echo GENERAL_LOG_PATH="$path"
    else
      log_error "failed to discover general_log path"
      return 1
    fi
  fi
}

logrotate_conf_vars_prepare_slow_query_log() {
  if [ "$SLOW_QUERY_LOG_DISCOVER_PATH" = "1" ]; then
    if path=$(mysql_global_variable_get slow_query_log_file) && [ -n "$path" ]; then
      echo SLOW_QUERY_LOG_PATH="$path"
    else
      log_error "failed to discover slow_query_log path"
      return 1
    fi
  fi
}

mysql_global_variable_get() {
  $MYSQL -sNe "SELECT @@global.$1"
}

# SIGTERM-handler
term_handler() {
  exit;
}

###
### PARSE CLI ARGUMENTS
###
### Very unfancy cli arg parsing. Only understands --[flag] [value]. Does not
### understand --[flag]=[value] or positional args.
###

while [ $# -gt 0 ]; do
  arg=$1
  case $arg in
    --audit-log-discover-path)
      AUDIT_LOG_DISCOVER_PATH=1
      ;;
    --audit-log-path)
      AUDIT_LOG_PATH=$2
      shift
      ;;
    --audit-log-rotate)
      AUDIT_LOG_ROTATE=$2
      shift
      ;;
    --audit-log-size)
      AUDIT_LOG_SIZE=$2
      shift
      ;;
    --create-group)
      CREATE_GROUP=$2
      shift
      ;;
    --create-user)
      CREATE_USER=$2
      shift
      ;;
    --default-rotate)
      DEFAULT_ROTATE=$2
      shift
      ;;
    --default-size)
      DEFAULT_SIZE=$2
      shift
      ;;
    --error-log-discover-path)
      ERROR_LOG_DISCOVER_PATH=1
      ;;
    --error-log-path)
      ERROR_LOG_PATH=$2
      shift
      ;;
    --error-log-rotate)
      ERROR_LOG_ROTATE=$2
      shift
      ;;
    --error-log-size)
      ERROR_LOG_SIZE=$2
      shift
      ;;
    --general-log-discover-path)
      GENERAL_LOG_DISCOVER_PATH=1
      ;;
    --general-log-path)
      GENERAL_LOG_PATH=$2
      shift
      ;;
    --general-log-rotate)
      GENERAL_LOG_ROTATE=$2
      shift
      ;;
    --general-log-size)
      GENERAL_LOG_SIZE=$2
      shift
      ;;
    --mysql-host)
      MYSQL_HOST=$2
      shift
      ;;
    --mysql-socket)
      MYSQL_SOCKET=$2
      shift
      ;;
    --mysql-user)
      MYSQL_USER=$2
      shift
      ;;
    --print-logrotate-conf)
      PRINT_LOGROTATE_CONF=1
      ;;
    --rotate-interval)
      ROTATE_INTERVAL=$2
      shift
      ;;
    --slow-query-log-discover-path)
      SLOW_QUERY_LOG_DISCOVER_PATH=1
      ;;
    --slow-query-log-path)
      SLOW_QUERY_LOG_PATH=$2
      shift
      ;;
    --slow-query-log-rotate)
      SLOW_QUERY_LOG_ROTATE=$2
      shift
      ;;
    --slow-query-log-size)
      SLOW_QUERY_LOG_SIZE=$2
      shift
      ;;
    --wildcard-path)
      WILDCARD_PATH=$2
      shift
      ;;
    --wildcard-rotate)
      WILDCARD_ROTATE=$2
      shift
      ;;
    --wildcard-size)
      WILDCARD_SIZE=$2
      shift
      ;;
  esac
  shift
done

###
### VALIDATE ARGS
###

if [ -n "$AUDIT_LOG_PATH" ] && [ "$AUDIT_LOG_DISCOVER_PATH" = "1" ]; then
  log_exit "mutually exclusive: [--audit-log-path, --audit-log-discover-path]"
fi

if [ -n "$ERROR_LOG_PATH" ] && [ "$ERROR_LOG_DISCOVER_PATH" = "1" ]; then
  log_exit "mutually exclusive: [--error-log-path, --error-log-discover-path]"
fi

if [ -n "$GENERAL_LOG_PATH" ] && [ "$GENERAL_LOG_DISCOVER_PATH" = "1" ]; then
  log_exit "mutually exclusive: [--general-log-path, --general-log-discover-path]"
fi

if [ -n "$SLOW_QUERY_LOG_PATH" ] && [ "$SLOW_QUERY_LOG_DISCOVER_PATH" = "1" ]; then
  log_exit "mutually exclusive: [--slow-query-log-path, --slow-query-log-discover-path]"
fi

###
### EXPORT VARS
###
### Export initial values for vars used by envsubst. Some of these variables
### (like GENERAL_LOG_PATH) may change dynamically.
###

export CREATE_GROUP
export CREATE_USER

MYSQL="/usr/bin/mysql --user=$MYSQL_USER"
if [ -n "$MYSQL_HOST" ]; then
  MYSQL="$MYSQL --host=$MYSQL_HOST"
elif [ -n "$MYSQL_SOCKET" ]; then
  MYSQL="$MYSQL --socket=$MYSQL_SOCKET"
fi
export MYSQL

### Audit log.

if [ -z "$AUDIT_LOG_PATH" ]; then
  AUDIT_LOG_PATH="${LOG_PATH_UNDEFINED}.audit"
fi
export AUDIT_LOG_PATH

if [ -z "$AUDIT_LOG_ROTATE" ]; then
  AUDIT_LOG_ROTATE="$DEFAULT_ROTATE"
fi
export AUDIT_LOG_ROTATE

if [ -z "$AUDIT_LOG_SIZE" ]; then
  AUDIT_LOG_SIZE="$DEFAULT_SIZE"
fi
export AUDIT_LOG_SIZE

### Error log.

if [ -z "$ERROR_LOG_PATH" ]; then
  ERROR_LOG_PATH="${LOG_PATH_UNDEFINED}.error"
fi
export ERROR_LOG_PATH

if [ -z "$ERROR_LOG_ROTATE" ]; then
  ERROR_LOG_ROTATE="$DEFAULT_ROTATE"
fi
export ERROR_LOG_ROTATE

if [ -z "$ERROR_LOG_SIZE" ]; then
  ERROR_LOG_SIZE="$DEFAULT_SIZE"
fi
export ERROR_LOG_SIZE

### General log.

if [ -z "$GENERAL_LOG_PATH" ]; then
  GENERAL_LOG_PATH="${LOG_PATH_UNDEFINED}.general"
fi
export GENERAL_LOG_PATH

if [ -z "$GENERAL_LOG_ROTATE" ]; then
  GENERAL_LOG_ROTATE="$DEFAULT_ROTATE"
fi
export GENERAL_LOG_ROTATE

if [ -z "$GENERAL_LOG_SIZE" ]; then
  GENERAL_LOG_SIZE="$DEFAULT_SIZE"
fi
export GENERAL_LOG_SIZE=$GENERAL_LOG_SIZE

### Slow query log.

if [ -z "$SLOW_QUERY_LOG_PATH" ]; then
  SLOW_QUERY_LOG_PATH="${LOG_PATH_UNDEFINED}.slow"
fi
export SLOW_QUERY_LOG_PATH

if [ -z "$SLOW_QUERY_LOG_ROTATE" ]; then
  SLOW_QUERY_LOG_ROTATE="$DEFAULT_ROTATE"
fi
export SLOW_QUERY_LOG_ROTATE

if [ -z "$SLOW_QUERY_LOG_SIZE" ]; then
  SLOW_QUERY_LOG_SIZE="$DEFAULT_SIZE"
fi
export SLOW_QUERY_LOG_SIZE

### Wildcard.

if [ -z "$WILDCARD_PATH" ]; then
  WILDCARD_PATH="$LOG_PATH_UNDEFINED"
fi
export WILDCARD_PATH

if [ -z "$WILDCARD_ROTATE" ]; then
  WILDCARD_ROTATE="$DEFAULT_ROTATE"
fi
export WILDCARD_ROTATE

if [ -z "$WILDCARD_SIZE" ]; then
  WILDCARD_SIZE="$DEFAULT_SIZE"
fi
export WILDCARD_SIZE

###
### MAIN
###
### Every $ROTATE_INTERVAL, regenerate $LOGROTATE_CONF and run $LOGROTATE.
###

# Setup handlers.
# On callback, kill the last background process, which is `tail -f /dev/null`
# and execute the specified handler.
trap 'kill ${!}; term_handler' SIGINT SIGTERM SIGHUP

while :; do
  if ! logrotate_conf_generate; then
    log_error "failed to generate logrotate conf"
  else
    if [ "$PRINT_LOGROTATE_CONF" = "1" ]; then
      cat "$LOGROTATE_CONF"
    fi

    # rotate logs
    "$LOGROTATE" -s "$LOGROTATE_STATUS" "$LOGROTATE_CONF"
  fi

  # Wait between logrotate invocations.
  sleep "$ROTATE_INTERVAL" & wait ${!}
done
