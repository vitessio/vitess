name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on: ubuntu-20.04

    steps:
    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/release-[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
          skip='true'
        fi
        echo Skip ${skip}
        echo "::set-output name=skip-workflow::${skip}"

    - name: Set up Go
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/setup-go@v2
      with:
        go-version: 1.17.13

    - name: Set up python
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/setup-python@v2

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v2

    - name: Get dependencies
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      run: |
        {{if .InstallXtraBackup}}

        # Setup Percona Server for MySQL 8.0
        sudo apt-get update
        sudo apt-get install -y lsb-release gnupg2 curl
        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo percona-release setup ps80
        sudo apt-get update

        # Install everything else we need, and configure
        sudo apt-get install -y percona-server-server percona-server-client make unzip g++ etcd git wget eatmydata xz-utils
        echo 'mysql version: '
        sudo mysql --version
        sudo service mysql stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
        {{else}}

        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29

        sudo apt-get update
        # stop any existing running instance of mysql
        sudo service mysql stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
        sudo systemctl stop apparmor

        # Uninstall any previously installed MySQL first
        sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-\*
        sudo apt-get -y autoremove
        sudo apt-get -y autoclean
        sudo deluser mysql
        sudo rm -rf /var/lib/mysql
        sudo rm -rf /etc/mysql

        # install necessary tools
        sudo apt-get install -y make unzip g++ etcd curl git wget eatmydata xz-utils libmecab2
        sudo service etcd stop

        # Download mysql8.0.25 (pin it)
        wget -c https://downloads.mysql.com/archives/get/p/23/file/mysql-8.0.25-linux-glibc2.17-x86_64-minimal.tar.xz
        # Untar the bundle. It will have all necessary deb
        sudo tar xf mysql-8.0.25-linux-glibc2.17-x86_64-minimal.tar.xz -v -C /usr
        echo 'mysql version: '
        sudo /usr/mysql-8.0.25-linux-glibc2.17-x86_64-minimal/bin/mysql --version
        {{end}}

        # configure rest
        sudo service etcd stop
        go mod download

        # install JUnit report formatter
        go get -u github.com/vitessio/go-junit-report@HEAD

        {{if .InstallXtraBackup}}

        sudo apt-get install percona-xtrabackup-80 lz4

        {{end}}

    {{if .MakeTools}}

    - name: Installing zookeeper and consul
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      run: |
          make tools

    {{end}}

    - name: Run cluster endtoend test
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      timeout-minutes: 45
      run: |
        export VT_MYSQL_ROOT="/usr/mysql-8.0.25-linux-glibc2.17-x86_64-minimal"
        source build.env

        set -x

        # run the tests however you normally do, then produce a JUnit XML file
        eatmydata -- go run test.go -docker=false -follow -shard {{.Shard}}