name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

env:
  LAUNCHABLE_ORGANIZATION: "vitess"
  LAUNCHABLE_WORKSPACE: "vitess-app"
  GITHUB_PR_HEAD_SHA: "${{`{{ github.event.pull_request.head.sha }}`}}"

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
        # Setup Percona Server for MySQL 8.0
        sudo apt-get update
        sudo apt-get install -y lsb-release gnupg2 curl
        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo percona-release setup ps80
        sudo apt-get update

        # Install everything else we need, and configure
        sudo apt-get install -y percona-server-server percona-server-client make unzip g++ etcd git wget eatmydata xz-utils
        sudo service mysql stop
        sudo service etcd stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
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
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 45
      run: |
        source build.env

        set -x

        {{if .LimitResourceUsage}}
        # Increase our local ephemeral port range as we could exhaust this
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 61999"
        # Increase our open file descriptor limit as we could hit this
        ulimit -n 65536
        cat <<-EOF>>./config/mycnf/mysql80.cnf
        innodb_buffer_pool_dump_at_shutdown=OFF
        innodb_buffer_pool_in_core_file=OFF
        innodb_buffer_pool_load_at_startup=OFF
        innodb_buffer_pool_size=64M
        innodb_doublewrite=OFF
        innodb_flush_log_at_trx_commit=0
        innodb_flush_method=O_DIRECT
        innodb_numa_interleave=ON
        innodb_adaptive_hash_index=OFF
        sync_binlog=0
        sync_relay_log=0
        performance_schema=OFF
        slow-query-log=OFF
        EOF
        {{end}}

        # run the tests however you normally do, then produce a JUnit XML file
        eatmydata -- go run test.go -docker={{if .Docker}}true -flavor={{.Platform}}{{else}}false{{end}} -follow -shard {{.Shard}}{{if .PartialKeyspace}} -partial-keyspace=true {{end}} | tee -a output.txt | go-junit-report -set-exit-code > report.xml

    - name: Print test output and Record test result in launchable
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true' && always()
      run: |
        # send recorded tests to launchable
        launchable record tests --build "$GITHUB_RUN_ID" go-test . || true

        # print test output
        cat output.txt
