name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on: self-hosted

    steps:
      - name: Check out code
        uses: actions/checkout@v2

      - name: Check for changes in relevant files
        uses: frouioui/paths-filter@main
        id: changes
        with:
          token: ''
          filters: |
            end_to_end:
              - 'go/**/*.go'
              - 'test.go'
              - 'Makefile'
              - 'build.env'
              - 'go.[sumod]'
              - 'proto/*.proto'
              - 'tools/**'
              - 'config/**'
              - '.github/docker/**'
              - 'bootstrap.sh'
              - '.github/workflows/**'

      - name: Build Docker Image
        if: steps.changes.outputs.end_to_end == 'true'
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}:$GITHUB_SHA  .

      - name: Run test
        if: steps.changes.outputs.end_to_end == 'true'
        timeout-minutes: 30
        run: docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'source build.env && go run test.go -keep-data=true -docker=false -print-log -follow -shard {{.Shard}} -- -- --keep-data=true'

      - name: Print Volume Used
        if: always() && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker inspect -f '{{"{{ (index .Mounts 0).Name }}"}}' {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Volume
        if: steps.changes.outputs.end_to_end == 'true'
        run: |
          docker rm -v {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Container
        if: always() && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker rm -f {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Image
        if: steps.changes.outputs.end_to_end == 'true'
        run: |
          docker image rm {{.ImageName}}:$GITHUB_SHA
