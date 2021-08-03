  - label: "{{.Name}}"
    command: . ./build.env && go run test.go -docker=false -print-log -follow -shard {{.Shard}}
    plugins:
      - docker-compose#v3.8.0:
          run: vitess
          config: {{.DockerCompose}}
          no-cache: true
          shell: ["/bin/bash", "-e", "-c"]
    timeout_in_minutes: 45
