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

jobs:
  build:
    timeout-minutes: 60
    name: Run endtoend tests on {{.Name}}
    runs-on: {{.RunsOn}}

    steps:
    - name: Harden the runner (Audit all outbound calls)
      uses: step-security/harden-runner@20cf305ff2072d973412fa9b1e3a4f227bda3c76 # v2.14.0
      with:
        egress-policy: audit

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
      uses: actions/checkout@8e8c483db84b4bee98b60c0593521ed34d9990e8 # v6.0.1
      with:
        persist-credentials: 'false'

    - name: Check for changes in relevant files
      uses: dorny/paths-filter@de90cc6fb38fc0963ad72b210f1f284cd68cea36 # v3.0.2
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
      uses: actions/setup-go@7a3fe6cf4cb3a834922a1244abfce67bcef6a0c5 # v6.2.0
      with:
        go-version-file: go.mod

{{if .GoPrivate}}
    - name: Setup GitHub access token
      if: steps.changes.outputs.end_to_end == 'true'
      run: git config --global url.https://${{`{{ secrets.GH_ACCESS_TOKEN }}`}}@github.com/.insteadOf https://github.com/
{{end}}

    - name: Set up python
      if: steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-python@83679a892e2d95755f2dac6acb0bfd1e9ac5d548 # v6.1.0

    - name: Tune the OS
      if: steps.changes.outputs.end_to_end == 'true'
      uses: ./.github/actions/tune-os

    {{if not .InstallXtraBackup}}
    - name: Setup MySQL
      if: steps.changes.outputs.end_to_end == 'true'
      uses: ./.github/actions/setup-mysql
      with:
        flavor: mysql-8.4
    {{ end }}

    - name: Get dependencies
      if: steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 10
      run: |
        {{if .InstallXtraBackup}}

        # Setup Percona Server for MySQL 8.0
        sudo apt-get -qq update
        sudo apt-get -qq install -y lsb-release gnupg2
        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo percona-release setup pdps8.0
        sudo apt-get -qq update

        sudo apt-get -qq install -y percona-server-server percona-server-client

        sudo service mysql stop

        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld

        sudo apt-get -qq install -y percona-xtrabackup-80 lz4

        {{else}}

        sudo apt-get -qq install -y mysql-shell

        {{end}}

        # Install everything else we need, and configure
        sudo apt-get -qq install -y make unzip g++ etcd-client etcd-server curl git wget xz-utils libncurses6

        sudo service etcd stop

        go mod download

        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@{{.GoJunitReport.SHA}} # {{.GoJunitReport.Comment}}

    {{if .NeedsMinio }}
    - name: Install Minio
      run: |
        wget https://dl.min.io/server/minio/release/linux-amd64/minio
        chmod +x minio
        mv minio /usr/local/bin
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
        # Increase our open file descriptor limit as we could hit this
        ulimit -n 65536
        cat <<-EOF>>./config/mycnf/mysql84.cnf
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

        {{if .EnableBinlogTransactionCompression}}
        cat <<-EOF>>./config/mycnf/mysql84.cnf
        binlog-transaction-compression=ON
        EOF
        {{end}}

        {{if .EnablePartialJSON}}
        cat <<-EOF>>./config/mycnf/mysql84.cnf
        binlog-row-value-options=PARTIAL_JSON
        EOF
        {{end}}

        # Some of these tests require specific locales to be installed.
        # See https://github.com/cncf/automation/commit/49f2ad7a791a62ff7d038002bbb2b1f074eed5d5
        # run the tests however you normally do, then produce a JUnit XML file
        go run test.go -docker={{if .Docker}}true -flavor={{.Platform}}{{else}}false{{end}} -follow -shard {{.Shard}}{{if .PartialKeyspace}} -partial-keyspace=true {{end}}{{if .BuildTag}} -build-tag={{.BuildTag}} {{end}} | tee -a output.txt | go-junit-report -set-exit-code > report.xml

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
