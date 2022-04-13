name: {{.Name}}
on: [push, pull_request]

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    {{if .Ubuntu20}}runs-on: ubuntu-20.04{{else}}runs-on: ubuntu-latest{{end}}

    steps:
    - name: Set up Go
      uses: actions/setup-go@v2
      with:
        go-version: 1.17.9

    - name: Tune the OS
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Check out code
      uses: actions/checkout@v2

    - name: Run cluster endtoend test
      timeout-minutes: 30
      run: |
        go run test.go -docker=true --follow -shard {{.Shard}}

