name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  test:
    runs-on: ubuntu-18.04

    steps:
    - name: Set up Go
      uses: actions/setup-go@v2
      with:
        go-version: 1.18

    - name: Tune the OS
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range
        # Increase the asynchronous non-blocking I/O. More information at https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_use_native_aio
        echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
        sudo sysctl -p /etc/sysctl.conf

    - name: Check out code
      uses: actions/checkout@v2

    - name: Get dependencies
      run: |
        export DEBIAN_FRONTEND="noninteractive"
        sudo apt-get update

        {{if (eq .Platform "mysql57")}}

        # mysql57
        sudo apt-get install -y mysql-server mysql-client

        {{else}}

        # !mysql57

        # Uninstall any previously installed MySQL first
        sudo systemctl stop apparmor
        sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-server mysql-client mysql-common
        sudo apt-get -y autoremove
        sudo apt-get -y autoclean
        sudo deluser mysql
        sudo rm -rf /var/lib/mysql
        sudo rm -rf /etc/mysql

        {{if (eq .Platform "mysql80")}}
        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29

        # mysql80
        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.14-1_all.deb
        echo mysql-apt-config mysql-apt-config/select-server select mysql-8.0 | sudo debconf-set-selections
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
        sudo apt-get update
        sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y mysql-server mysql-client

        {{end}}

        {{if (eq .Platform "mariadb101")}}

        # mariadb101
        sudo apt-get install -y software-properties-common
        sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
        sudo add-apt-repository 'deb [arch=amd64,arm64,ppc64el] http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.1/ubuntu bionic main'
        sudo apt update
        sudo DEBIAN_FRONTEND="noninteractive" apt install -y mariadb-server

        {{end}}

        {{if (eq .Platform "mariadb102")}}

        # mariadb102
        sudo apt-get install -y software-properties-common
        sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
        sudo add-apt-repository 'deb [arch=amd64,arm64,ppc64el] http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.2/ubuntu bionic main'
        sudo apt update
        sudo DEBIAN_FRONTEND="noninteractive" apt install -y mariadb-server

        {{end}}

        {{if (eq .Platform "mariadb103")}}

        # mariadb103
        sudo apt-get install -y software-properties-common
        sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
        sudo add-apt-repository 'deb [arch=amd64,arm64,ppc64el] http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.3/ubuntu bionic main'
        sudo apt update
        sudo DEBIAN_FRONTEND="noninteractive" apt install -y mariadb-server

        {{end}}

        {{end}} {{/*outer if*/}}

        sudo apt-get install -y make unzip g++ curl git wget ant openjdk-8-jdk eatmydata
        sudo service mysql stop
        sudo bash -c "echo '/usr/sbin/mysqld { }' > /etc/apparmor.d/usr.sbin.mysqld" # https://bugs.launchpad.net/ubuntu/+source/mariadb-10.1/+bug/1806263
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld || echo "could not remove mysqld profile"

        mkdir -p dist bin
        curl -L https://github.com/coreos/etcd/releases/download/v3.3.10/etcd-v3.3.10-linux-amd64.tar.gz | tar -zxC dist
        mv dist/etcd-v3.3.10-linux-amd64/{etcd,etcdctl} bin/

        go mod download
        go install golang.org/x/tools/cmd/goimports@latest

    - name: Run make tools
      run: |
        make tools

    - name: Run test
      timeout-minutes: 30
      run: |
        eatmydata -- make unit_test
