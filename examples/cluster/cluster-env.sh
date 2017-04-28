hostname=`hostname -f`
vtctld_web_port=15000

# Add all hosts here in a similar fashion 
zkcfg=(\
    "1@10.124.117.95:2181:2888:3888" \
    "2@10.124.117.88:2181:2888:3888" \
    )
printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}
ZK_SERVER=$(echo $zkcfg | awk -F'[@,:]' '{print $2":"$3","$7":"$8}')

zkids=$(echo $zkcfg | awk 'gsub(/@[^,]*/,"")' | awk 'gsub(/,/," ")')

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

# Set topology environment parameters.
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address $ZK_SERVER -topo_global_root /vitess/global"

mkdir -p $VTDATAROOT/tmp
