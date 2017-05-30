hostname=`hostname -f`
vtctld_web_port=15000
vtctld_host="vtmaster1.dca.applift.com"
cell='applift'
keyspace='fcapdb'

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/zk.cfg

printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}
ZK_SERVER=$(echo $zkcfg | awk -F'[@,:]' '{out=""; for(i=2;i<=NF;i+=5){out=out","$i":"$(i+1)}; print substr(out,2)}')
zkids=$(echo $zkcfg | awk 'gsub(/@[^,]*/,"")' | awk 'gsub(/,/," ")')

# Set up environment.
export VTROOT=/opt/vitess/
export VTDATAROOT=/opt/vitess/vtdataroot
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
