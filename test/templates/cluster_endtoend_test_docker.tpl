name: {{.Name}}
on: [push, pull_request]

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    {{if .Ubuntu20}}runs-on: ubuntu-20.04{{else}}runs-on: ubuntu-latest{{end}}

    steps:
    - name: Check out code
      uses: actions/checkout@v2

    - name: Check for changes in relevant files
      uses: dorny/paths-filter@v2
      id: changes
      with:
        filters: |
          end_to_end:
            - 'go/**'

    - name: Set up Go
      if: steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-go@v2
      with:
        go-version: 1.18

    - name: Tune the OS
      if: steps.changes.outputs.end_to_end == 'true'
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Run cluster endtoend test
      if: steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 30
      run: |
        go run test.go -docker=true --follow -shard {{.Shard}}
