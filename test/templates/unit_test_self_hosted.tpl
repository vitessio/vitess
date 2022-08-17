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

      - name: Build Docker Image
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}:$GITHUB_SHA  .

      - name: Run test
        uses: nick-fields/retry@v2
        with:
          timeout_minutes: 30
          max_attempts: 3
          retry_on: error
          command: |
            docker run --name "{{.ImageName}}_$GITHUB_SHA" {{.ImageName}}:$GITHUB_SHA /bin/bash -c 'make unit_test'

      - name: Print Volume Used
        if: ${{"{{ always() }}"}}
        run: |
          docker inspect -f '{{"{{ (index .Mounts 0).Name }}"}}' {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Volume
        run: |
          docker rm -v {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Container
        if: ${{"{{ always() }}"}}
        run: |
          docker rm -f {{.ImageName}}_$GITHUB_SHA

      - name: Cleanup Docker Image
        run: |
          docker image rm {{.ImageName}}:$GITHUB_SHA
