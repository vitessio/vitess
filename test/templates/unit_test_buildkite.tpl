  - label: "{{.Name}}"
    commands:
        - ./.buildkite/setup_go.sh
        - echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range
    timeout_in_minutes: 30
