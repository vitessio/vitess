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
    name: Run endtoend tests on {{.Name}}
    runs-on: {{.RunsOn}}

    steps:
    - name: Skip CI
      run: |
        if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
          echo "skipping CI due to the 'Skip CI' label"
          exit 1
        fi

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
            - 'go/test/endtoend/vtgate/vitess_tester/**'
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
        # Limit local port range to not use ports that overlap with server side
        # ports that we listen on.
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
        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C
        # Setup MySQL 8.4
        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.34-1_all.deb
        echo mysql-apt-config mysql-apt-config/select-server select mysql-8.4-lts | sudo debconf-set-selections
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
        sudo apt-get -qq update
        # Install everything else we need, and configure
        sudo apt-get -qq install -y mysql-server mysql-client make unzip g++ etcd-client etcd-server curl git wget eatmydata xz-utils libncurses6

        sudo service mysql stop
        sudo service etcd stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
        go mod download

        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@HEAD
        
        # install vitess tester
        go install github.com/vitessio/vt/go/vt@e43009309f599378504905d4b804460f47822ac5

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
        export NOVTADMINBUILD=1
        make build

        set -exo pipefail

        i=1
        for dir in {{.Path}}/*/; do 
          # We go over all the directories in the given path.
          # If there is a vschema file there, we use it, otherwise we let vt tester autogenerate it.
          if [ -f $dir/vschema.json ]; then
            vt tester --xunit --vschema "$dir"vschema.json $dir/*.test
          else 
            vt tester --sharded --xunit $dir/*.test
          fi
          # Number the reports by changing their file names.
          mv report.xml report"$i".xml
          i=$((i+1))
        done

    - name: Record test results in launchable if PR is not a draft
      if: github.event_name == 'pull_request' && github.event.pull_request.draft == 'false' && steps.changes.outputs.end_to_end == 'true' && github.base_ref == 'main' && !cancelled()
      run: |
        # send recorded tests to launchable
        launchable record tests --build "$GITHUB_RUN_ID" go-test . || true

    - name: Print test output
      if: steps.changes.outputs.end_to_end == 'true' && !cancelled()
      run: |
        # print test output
        cat report*.xml

    - name: Test Summary
      if: steps.changes.outputs.end_to_end == 'true' && !cancelled()
      uses: test-summary/action@31493c76ec9e7aa675f1585d3ed6f1da69269a86 # v2.4
      with:
        paths: "report*.xml"
        show: "fail"
