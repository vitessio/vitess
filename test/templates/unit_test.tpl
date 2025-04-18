name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

permissions: read-all

env:
  LAUNCHABLE_ORGANIZATION: "vitess"
  LAUNCHABLE_WORKSPACE: "vitess-app"
  GITHUB_PR_HEAD_SHA: "${{`{{ github.event.pull_request.head.sha }}`}}"
  GOPRIVATE: github.com/slackhq/vitess-addons
  GH_ACCESS_TOKEN: "${{`{{ secrets.GH_ACCESS_TOKEN }}`}}"

jobs:
  test:
    name: {{.Name}}
    runs-on: vitess-ubuntu24-16cpu-1

    steps:
    - name: Skip CI
      run: |
        if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
          echo "skipping CI due to the 'Skip CI' label"
          exit 1
        fi

    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        echo Skip ${skip}
        echo "skip-workflow=${skip}" >> $GITHUB_OUTPUT

        PR_DATA=$(curl \
          -H "{{"Authorization: token ${{ secrets.GITHUB_TOKEN }}"}}" \
          -H "Accept: application/vnd.github.v3+json" \
          "{{"https://api.github.com/repos/${{ github.repository }}/pulls/${{ github.event.pull_request.number }}"}}")
        draft=$(echo "$PR_DATA" | jq .draft -r)
        echo "is_draft=${draft}" >> $GITHUB_OUTPUT

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v4

    - name: Check for changes in relevant files
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: dorny/paths-filter@v3.0.1
      id: changes
      with:
        token: ''
        filters: |
          unit_tests:
            - 'go/**'
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
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      uses: actions/setup-go@v5
      with:
        go-version: 1.22.8

    - name: Setup github.com/slackhq/vitess-additions access token
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: git config --global url.https://$GH_ACCESS_TOKEN@github.com/.insteadOf https://github.com/

    - name: Set up python
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      uses: actions/setup-python@v5

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: |
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 65535"
        # Increase the asynchronous non-blocking I/O. More information at https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_use_native_aio
        echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
        sudo sysctl -p /etc/sysctl.conf

    - name: Get dependencies
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: |
        export DEBIAN_FRONTEND="noninteractive"
        sudo apt-get update

        # Uninstall any previously installed MySQL first
        sudo systemctl stop apparmor
        sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-server mysql-client mysql-common
        sudo apt-get -y autoremove
        sudo apt-get -y autoclean
        sudo deluser mysql
        sudo rm -rf /var/lib/mysql
        sudo rm -rf /etc/mysql

        {{if (eq .Platform "mysql57")}}
        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C
        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.33-1_all.deb
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
        # libtinfo5 is also needed for older MySQL 5.7 builds.
        curl -L -O http://mirrors.kernel.org/ubuntu/pool/universe/n/ncurses/libtinfo5_6.3-2ubuntu0.1_amd64.deb
        sudo dpkg -i libtinfo5_6.3-2ubuntu0.1_amd64.deb
        sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y mysql-client=5.7* mysql-community-server=5.7* mysql-server=5.7* libncurses6
        {{end}}

        {{if (eq .Platform "mysql80")}}
        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C

        # mysql80
        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb
        echo mysql-apt-config mysql-apt-config/select-server select mysql-8.0 | sudo debconf-set-selections
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
        sudo apt-get update
        sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y mysql-server mysql-client

        {{end}}

        sudo apt-get install -y make unzip g++ curl git wget ant openjdk-11-jdk eatmydata
        sudo service mysql stop
        sudo bash -c "echo '/usr/sbin/mysqld { }' > /etc/apparmor.d/usr.sbin.mysqld" # https://bugs.launchpad.net/ubuntu/+source/mariadb-10.1/+bug/1806263
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld || echo "could not remove mysqld profile"

        mkdir -p dist bin
        curl -L https://github.com/coreos/etcd/releases/download/v3.3.10/etcd-v3.3.10-linux-amd64.tar.gz | tar -zxC dist
        mv dist/etcd-v3.3.10-linux-amd64/{etcd,etcdctl} bin/

        go mod download
        go install golang.org/x/tools/cmd/goimports@latest
        
        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@HEAD

    - name: Run make tools
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: |
        make tools

    - name: Setup launchable dependencies
      if: steps.skip-workflow.outputs.is_draft == 'false' && steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true' && github.base_ref == 'main'
      run: |
        # Get Launchable CLI installed. If you can, make it a part of the builder image to speed things up
        pip3 install --user launchable~=1.0 > /dev/null

        # verify that launchable setup is all correct.
        launchable verify || true

        # Tell Launchable about the build you are producing and testing
        launchable record build --name "$GITHUB_RUN_ID" --no-commit-collection --source .

    - name: Run test
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      timeout-minutes: 30
      run: |
        set -exo pipefail
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"

        export NOVTADMINBUILD=1
        eatmydata -- make unit_test | tee -a output.txt | go-junit-report -set-exit-code > report.xml

    - name: Print test output and Record test result in launchable if PR is not a draft
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true' && always()
      run: |
        if [[ "{{"${{steps.skip-workflow.outputs.is_draft}}"}}" ==  "false" ]]; then
          # send recorded tests to launchable
          launchable record tests --build "$GITHUB_RUN_ID" go-test . || true
        fi

        # print test output
        cat output.txt
