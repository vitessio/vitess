name: {{.Name}}
on: [push, pull_request]

jobs:
  test:
    runs-on: self-hosted

    steps:
      - name: Check out code
        uses: actions/checkout@v2

      - name: Build Docker Image
        run: docker build -f {{.Dockerfile}} -t {{.ImageName}}  .

      - name: Run test
        timeout-minutes: 30
        run: docker run --rm {{.ImageName}} /bin/bash -c 'source build.env && go run test.go -docker=false -print-log -follow -shard {{.Shard}}'

      - name: Cleanup Docker Image
        run: |
          docker images rm {{.ImageName}}
          docker volume prune -f
