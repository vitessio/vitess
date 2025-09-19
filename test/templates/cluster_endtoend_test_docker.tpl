name: {{.Name}}
on:
  push:
    branches:
      - "main"
      - "release-[0-9]+.[0-9]"
    tags: '**'
  pull_request:
    branches: '**'

permissions: read-all

env:
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

    - name: Tune the OS
      if: steps.changes.outputs.end_to_end == 'true'
      run: |
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 65535"

    - name: Run cluster endtoend test
      if: steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 30
      run: |
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"

        go run test.go -docker=true --follow -shard {{.Shard}}
