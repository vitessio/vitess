name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on:
      group: vitess-ubuntu20
    env:
      GOPRIVATE: github.com/slackhq/vitess-addons
      GH_ACCESS_TOKEN: {{"${{ secrets.GH_ACCESS_TOKEN }}"}}

    steps:
      - name: Check if workflow needs to be skipped
        id: skip-workflow
        run: |
          skip='false'
          echo Skip ${skip}
          echo "skip-workflow=${skip}" >> $GITHUB_OUTPUT

      - name: Check out code
        if: steps.skip-workflow.outputs.skip-workflow == 'false'
        uses: actions/checkout@v3

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
        uses: nick-fields/retry@v2
        with:
          timeout_minutes: 30
          max_attempts: 3
          retry_on: error
          command: |
            # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
            # which musn't be more than 107 characters long.
            export VTDATAROOT="/tmp/"

            docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'source build.env && go run test.go -keep-data=true -docker=false -print-log -follow -shard {{.Shard}} -- -- --keep-data=true'

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
