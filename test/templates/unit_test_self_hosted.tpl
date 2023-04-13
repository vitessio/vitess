name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

permissions: read-all

jobs:
  test:
    runs-on: self-hosted

    steps:
      - name: Skip CI
        run: |
          if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
            echo "skipping CI due to the 'Skip CI' label"
            exit 1
          fi

      - name: Check if workflow needs to be skipped
        id: skip-workflow
        run: |
          skip='false'
          if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/release-[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
            skip='true'
          fi
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
            unit_tests:
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

      - name: Build Docker Image
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}:$GITHUB_SHA  .

      - name: Run test
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        timeout-minutes: 30
        run: docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'make unit_test'

      - name: Print Volume Used
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        if: ${{"{{ always() }}"}}
        run: |
          docker inspect -f '{{"{{ (index .Mounts 0).Name }}"}}' {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Volume
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        run: |
          docker rm -v {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Container
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        if: ${{"{{ always() }}"}}
        run: |
          docker rm -f {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Image
        if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
        run: |
          docker image rm {{.ImageName}}:$GITHUB_SHA
