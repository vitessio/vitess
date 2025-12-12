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
  test:
    name: {{.Name}}
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
          unit_tests:
            - 'test/config.json'
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
      if: steps.changes.outputs.unit_tests == 'true'
      uses: actions/setup-go@d35c59abb061a4a6fb18e82ac0862c26744d6ab5 # v5.5.0
      with:
        go-version-file: go.mod

{{if .GoPrivate}}
    - name: Setup GitHub access token
      if: steps.changes.outputs.unit_tests == 'true'
      run: git config --global url.https://${{`{{ secrets.GH_ACCESS_TOKEN }}`}}@github.com/.insteadOf https://github.com/
{{end}}

    - name: Set up python
      if: steps.changes.outputs.unit_tests == 'true'
      uses: actions/setup-python@39cd14951b08e74b54015e9e001cdefcf80e669f # v5.1.1

    - name: Tune the OS
      if: steps.changes.outputs.unit_tests == 'true'
      uses: ./.github/actions/tune-os

    - name: Setup MySQL
      if: steps.changes.outputs.unit_tests == 'true'
      uses: ./.github/actions/setup-mysql
      with:
        {{ if (eq .Platform "mysql57") -}}
        flavor: mysql-5.7
        {{ end }}
        {{- if (eq .Platform "mysql80") -}}
        flavor: mysql-8.0
        {{ end }}
        {{- if (eq .Platform "mysql84") -}}
        flavor: mysql-8.4
        {{ end }}

    - name: Get dependencies
      if: steps.changes.outputs.unit_tests == 'true'
      run: |
        export DEBIAN_FRONTEND="noninteractive"
        sudo apt-get install -y make unzip g++ curl git wget ant openjdk-11-jdk

        mkdir -p dist bin
        curl --max-time 10 --retry 3 --retry-max-time 45 -s -L https://github.com/coreos/etcd/releases/download/v3.5.25/etcd-v3.5.25-linux-amd64.tar.gz | tar -zxC dist
        mv dist/etcd-v3.5.25-linux-amd64/{etcd,etcdctl} bin/

        go mod download
        go install golang.org/x/tools/cmd/goimports@{{.Goimports.SHA}} # {{.Goimports.Comment}}

        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@{{.GoJunitReport.SHA}} # {{.GoJunitReport.Comment}}

    - name: Run make tools
      if: steps.changes.outputs.unit_tests == 'true'
      run: |
        make tools

    - name: Setup launchable dependencies
      if: github.event_name == 'pull_request' && github.event.pull_request.draft == 'false' && steps.changes.outputs.unit_tests == 'true' && github.base_ref == 'main'
      run: |
        # Get Launchable CLI installed. If you can, make it a part of the builder image to speed things up
        pip3 install --user launchable~=1.0 > /dev/null

        # verify that launchable setup is all correct.
        launchable verify || true

        # Tell Launchable about the build you are producing and testing
        launchable record build --name "$GITHUB_RUN_ID" --no-commit-collection --source .

    - name: Run test
      if: steps.changes.outputs.unit_tests == 'true'
      timeout-minutes: 30
      run: |
        set -exo pipefail
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"

        export NOVTADMINBUILD=1
        export VT_GO_PARALLEL_VALUE=$(nproc)
        export VTEVALENGINETEST="{{.Evalengine}}"
        # We sometimes need to alter the behavior based on the platform we're
        # testing, e.g. MySQL 5.7 vs 8.0.
        export CI_DB_PLATFORM="{{.Platform}}"

        make unit_test | tee -a output.txt | go-junit-report -set-exit-code > report.xml

    - name: Record test results in launchable if PR is not a draft
      if: github.event_name == 'pull_request' && github.event.pull_request.draft == 'false' && steps.changes.outputs.unit_tests == 'true' && github.base_ref == 'main' && !cancelled()
      run: |
        # send recorded tests to launchable
        launchable record tests --build "$GITHUB_RUN_ID" go-test . || true

    - name: Print test output
      if: steps.changes.outputs.unit_tests == 'true' && !cancelled()
      run: |
        # print test output
        cat output.txt

    - name: Test Summary
      if: steps.changes.outputs.unit_tests == 'true' && !cancelled()
      uses: test-summary/action@31493c76ec9e7aa675f1585d3ed6f1da69269a86 # v2.4
      with:
        paths: "report.xml"
        show: "fail"
