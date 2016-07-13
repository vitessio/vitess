hostname=`hostname -f`

# Each ZooKeeper server needs a list of all servers in the quorum.
# Since we're running them all locally, we need to give them unique ports.
# In a real deployment, these should be on different machines, and their
# respective hostnames should be given.
zkcfg=(\
    "1@$hostname:28881:38881:21811" \
    "2@$hostname:28882:38882:21812" \
    "3@$hostname:28883:38883:21813" \
    )
printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}

zkids='1 2 3'

# Set up environment.
export VTTOP=$VTROOT/src/github.com/youtube/vitess

# Try to find mysqld_safe on PATH.
if [ -z "$VT_MYSQL_ROOT" ]; then
  mysql_path=`which mysqld_safe`
  if [ -z "$mysql_path" ]; then
    echo "Can't guess location of mysqld_safe. Please set VT_MYSQL_ROOT so it can be found at \$VT_MYSQL_ROOT/bin/mysqld_safe."
    exit 1
  fi
  export VT_MYSQL_ROOT=$(dirname `dirname $mysql_path`)
fi

# We expect to find zk-client-conf.json in the same folder as this script.
env_script_root=`dirname "${BASH_SOURCE}"`
export ZK_CLIENT_CONFIG=$env_script_root/zk-client-conf.json

mkdir -p $VTDATAROOT/tmp

