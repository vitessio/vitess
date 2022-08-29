name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    {{if .Ubuntu20}}runs-on: ubuntu-20.04{{else}}runs-on: ubuntu-18.04{{end}}

    steps:
    - name: Set up Go
      uses: actions/setup-go@v2
      with:
        go-version: 1.17.13

    - name: Tune the OS
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Check out code
      uses: actions/checkout@v2

    - name: Get dependencies
      run: |
        sudo apt-get update
        sudo apt-get install -y mysql-server mysql-client make unzip g++ etcd curl git wget eatmydata
        sudo service mysql stop
        sudo service etcd stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
        go mod download

        {{if .InstallXtraBackup}}

        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo apt-get install -y gnupg2
        sudo dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo apt-get update
        sudo apt-get install percona-xtrabackup-24

        {{end}}

    {{if .MakeTools}}

    - name: Installing zookeeper and consul
      run: |
          make tools

    {{end}}

    - name: Run cluster endtoend test
      timeout-minutes: 30
      run: |
        source build.env
        eatmydata -- go run test.go -docker=false -print-log -follow -shard {{.Shard}}
