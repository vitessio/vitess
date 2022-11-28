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
      - name: Check if workflow needs to be skipped
        id: skip-workflow
        run: |
          skip='false'
          if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/release-[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
            skip='true'
          fi
          echo Skip ${skip}
          echo "::set-output name=skip-workflow::${skip}"

      - name: Check out code
        if: steps.skip-workflow.outputs.skip-workflow == 'false'
        uses: actions/checkout@v2

      - name: Check for changes in relevant files
        if: steps.skip-workflow.outputs.skip-workflow == 'false'
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
              - 'go.sum'
              - 'go.mod'
              - 'proto/*.proto'
              - 'tools/**'
              - 'config/**'
              - '.github/docker/**'
              - 'bootstrap.sh'
              - '.github/workflows/{{.FileName}}'

      - name: Build Docker Image
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}:$GITHUB_SHA  .

      - name: Run test
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        timeout-minutes: 30
        run: docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'source build.env && go run test.go -keep-data=true -docker=false -print-log -follow -shard {{.Shard}} -- -- --keep-data=true'

      - name: Print Volume Used
        if: always() && steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker inspect -f '{{"{{ (index .Mounts 0).Name }}"}}' {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Volume
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker rm -v {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Container
        if: always() && steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker rm -f {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Image
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
        run: |
          docker image rm {{.ImageName}}:$GITHUB_SHA
