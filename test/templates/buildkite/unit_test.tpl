  - label: "{{.Name}}"
    command: make unit_test
    plugins:
      - docker-compose#v3.8.0:
          run: vitess
          config: {{.DockerCompose}}
          no-cache: true
    timeout_in_minutes: 30
