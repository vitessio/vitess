name: {{.Name}}
on: [push, pull_request]

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    {{if .Ubuntu20}}runs-on: ubuntu-20.04{{else}}runs-on: ubuntu-latest{{end}}

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

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v2

    - name: Run cluster endtoend test
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      timeout-minutes: 30
      run: |
        go run test.go -docker=true --follow -shard {{.Shard}}

