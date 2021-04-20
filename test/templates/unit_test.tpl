name: {{.Name}}
on: [push, pull_request]
jobs:

  test:
    runs-on: ubuntu-18.04

    steps:
    - name: Set up Go
      uses: actions/setup-go@v1
      with:
        go-version: 1.15

    - name: Tune the OS
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    # TEMPORARY WHILE GITHUB FIXES THIS https://github.com/actions/virtual-environments/issues/3185
    - name: Add the current IP address, long hostname and short hostname record to /etc/hosts file
      run: |
        echo -e "$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)\t$(hostname -f) $(hostname -s)" | sudo tee -a /etc/hosts
    # DON'T FORGET TO REMOVE CODE ABOVE WHEN ISSUE IS ADRESSED!

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

        {{if (eq .Platform "percona56")}}

        # percona56
        sudo rm -rf /var/lib/mysql
        sudo apt install -y gnupg2
        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo apt update
        sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y percona-server-server-5.6 percona-server-client-5.6

        {{end}}

        {{if (eq .Platform "mysql80")}}

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

    - name: Run make tools
      run: |
        make tools

    - name: Run test
      timeout-minutes: 30
      run: |
        eatmydata -- make unit_test
