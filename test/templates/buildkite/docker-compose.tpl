version: "3.8"
services:
  vitess:
    build:
      context: ../../
      dockerfile: {{.Dockerfile}}
