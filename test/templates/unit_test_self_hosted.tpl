name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  test:
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
            unit_tests:
              - 'go/**'
              - 'test.go'
              - 'Makefile'
              - 'build.env'
              - 'go.[sumod]'
              - 'proto/*.proto'
              - 'tools/**'
              - 'config/**'
              - 'bootstrap.sh'
              - '.github/workflows/**'

      - name: Build Docker Image
        if: steps.changes.outputs.unit_tests == 'true'
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}:$GITHUB_SHA  .

      - name: Run test
        if: steps.changes.outputs.unit_tests == 'true'
        timeout-minutes: 30
        run: docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'make unit_test'

      - name: Print Volume Used
        if: steps.changes.outputs.unit_tests == 'true'
        if: ${{"{{ always() }}"}}
        run: |
          docker inspect -f '{{"{{ (index .Mounts 0).Name }}"}}' {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Volume
        if: steps.changes.outputs.unit_tests == 'true'
        run: |
          docker rm -v {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Container
        if: steps.changes.outputs.unit_tests == 'true'
        if: ${{"{{ always() }}"}}
        run: |
          docker rm -f {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Image
        if: steps.changes.outputs.unit_tests == 'true'
        run: |
          docker image rm {{.ImageName}}:$GITHUB_SHA
