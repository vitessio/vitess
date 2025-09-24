name: {{.Name}}
on:
  push:
    branches:
      - "main"
      - "release-[0-9]+.[0-9]"
    tags: '**'
  pull_request:
    branches: '**'
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

permissions: read-all

env:
  LAUNCHABLE_ORGANIZATION: "vitess"
  LAUNCHABLE_WORKSPACE: "vitess-app"
  GITHUB_PR_HEAD_SHA: "${{`{{ github.event.pull_request.head.sha }}`}}"
{{if .GoPrivate}}  GOPRIVATE: "{{.GoPrivate}}"{{end}}
{{if .InstallXtraBackup}}
  # This is used if we need to pin the xtrabackup version used in tests.
  # If this is NOT set then the latest version available will be used.
  #XTRABACKUP_VERSION: "2.4.24-1"
{{end}}

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on: {{.RunsOn}}

    steps:
    - name: Skip CI
      run: |
        if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
          echo "skipping CI due to the 'Skip CI' label"
          exit 1
        fi

    {{if .MemoryCheck}}

    - name: Check Memory
      run: |
        totalMem=$(free -g | awk 'NR==2 {print $2}')
        echo "total memory $totalMem GB"
        if [[ "$totalMem" -lt 15 ]]; then 
          echo "Less memory than required"
          exit 1
        fi

    {{end}}

    - name: Check out code
      uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2
      with:
        persist-credentials: 'false'

    - name: Check for changes in relevant files
      uses: dorny/paths-filter@ebc4d7e9ebcb0b1eb21480bb8f43113e996ac77a # v3.0.1
      id: changes
      with:
        token: ''
        filters: |
          end_to_end:
            - 'test/config.json'
            - 'go/**/*.go'
            - 'go/vt/sidecardb/**/*.sql'
            - 'go/test/endtoend/onlineddl/vrepl_suite/**'
            - 'test.go'
            - 'Makefile'
            - 'build.env'
            - 'go.sum'
            - 'go.mod'
            - 'proto/*.proto'
            - 'tools/**'
            - 'config/**'
            - 'bootstrap.sh'
            - '.github/workflows/{{.FileName}}'
            {{- if or (contains .Name "onlineddl") (contains .Name "schemadiff") }}
            - 'go/test/endtoend/onlineddl/vrepl_suite/testdata'
            {{- end}}

    - name: Set up Go
      if: steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-go@d35c59abb061a4a6fb18e82ac0862c26744d6ab5 # v5.5.0
      with:
        go-version-file: go.mod

{{if .GoPrivate}}
    - name: Setup GitHub access token
      if: steps.changes.outputs.end_to_end == 'true'
      run: git config --global url.https://${{`{{ secrets.GH_ACCESS_TOKEN }}`}}@github.com/.insteadOf https://github.com/
{{end}}

    - name: Set up python
      if: steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-python@39cd14951b08e74b54015e9e001cdefcf80e669f # v5.1.1

    - name: Tune the OS
      if: steps.changes.outputs.end_to_end == 'true'
      run: |
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 65535"

        # Increase the asynchronous non-blocking I/O. More information at https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_use_native_aio
        echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
        sudo sysctl -p /etc/sysctl.conf

        # Don't waste a bunch of time processing man-db triggers
        echo "set man-db/auto-update false" | sudo debconf-communicate
        sudo dpkg-reconfigure man-db


    - name: Get dependencies
      if: steps.changes.outputs.end_to_end == 'true'
      run: |
        sudo apt-get update

        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld

        # sudo systemctl stop apparmor
        sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-server mysql-client mysql-common
        sudo apt-get -y autoremove
        sudo apt-get -y autoclean
        # sudo deluser mysql
        sudo rm -rf /var/lib/mysql
        sudo rm -rf /etc/mysql

        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C

        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.34-1_all.deb
        # Bionic packages are still compatible for Jammy since there's no MySQL 5.7
        # packages for Jammy.
        echo mysql-apt-config mysql-apt-config/repo-codename select bionic | sudo debconf-set-selections
        echo mysql-apt-config mysql-apt-config/select-server select mysql-5.7 | sudo debconf-set-selections
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
        sudo apt-get update
        # We have to install this old version of libaio1. See also:
        # https://bugs.launchpad.net/ubuntu/+source/libaio/+bug/2067501
        curl -L -O http://mirrors.kernel.org/ubuntu/pool/main/liba/libaio/libaio1_0.3.112-13build1_amd64.deb
        sudo dpkg -i libaio1_0.3.112-13build1_amd64.deb
        sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y mysql-client=5.7* mysql-community-server=5.7* mysql-server=5.7* libncurses6

        sudo apt-get install -y make unzip g++ etcd-client etcd-server curl git wget eatmydata

        sudo service mysql stop
        sudo service etcd stop

        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@HEAD

        {{if .InstallXtraBackup}}

        wget "https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb"
        sudo apt-get install -y gnupg2
        sudo dpkg -i "percona-release_latest.$(lsb_release -sc)_all.deb"
        sudo percona-release enable-only tools
        sudo apt-get update
        if [[ -n $XTRABACKUP_VERSION ]]; then
          debfile="percona-xtrabackup-24_$XTRABACKUP_VERSION.$(lsb_release -sc)_amd64.deb"
          wget "https://repo.percona.com/pxb-24/apt/pool/main/p/percona-xtrabackup-24/$debfile"
          sudo apt install -y "./$debfile"
        else
          sudo apt-get install -y percona-xtrabackup-24
        fi

        {{end}}

    {{if .MakeTools}}

    - name: Installing zookeeper and consul
      if: steps.changes.outputs.end_to_end == 'true'
      run: |
          make tools

    {{end}}

    - name: Setup launchable dependencies
      if: github.event_name == 'pull_request' && github.event.pull_request.draft == 'false' && steps.changes.outputs.end_to_end == 'true' && github.base_ref == 'main'
      run: |
        # Get Launchable CLI installed. If you can, make it a part of the builder image to speed things up
        pip3 install --user launchable~=1.0 > /dev/null

        # verify that launchable setup is all correct.
        launchable verify || true

        # Tell Launchable about the build you are producing and testing
        launchable record build --name "$GITHUB_RUN_ID" --no-commit-collection --source .

    - name: Run cluster endtoend test
      if: steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 45
      run: |
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"
        source build.env

        set -exo pipefail

        {{if .LimitResourceUsage}}
        # Increase our local ephemeral port range as we could exhaust this
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 61999"
        # Increase our open file descriptor limit as we could hit this
        ulimit -n 65536
        cat <<-EOF>>./config/mycnf/mysql57.cnf
        innodb_buffer_pool_dump_at_shutdown=OFF
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

    - name: Record test results in launchable if PR is not a draft
      if: github.event_name == 'pull_request' && github.event.pull_request.draft == 'false' && steps.changes.outputs.end_to_end == 'true' && github.base_ref == 'main' && !cancelled()
      run: |
        # send recorded tests to launchable
        launchable record tests --build "$GITHUB_RUN_ID" go-test . || true

    - name: Print test output
      if: steps.changes.outputs.end_to_end == 'true' && !cancelled()
      run: |
        # print test output
        cat output.txt

    - name: Test Summary
      if: steps.changes.outputs.end_to_end == 'true' && !cancelled()
      uses: test-summary/action@31493c76ec9e7aa675f1585d3ed6f1da69269a86 # v2.4
      with:
        paths: "report.xml"
        show: "fail"
