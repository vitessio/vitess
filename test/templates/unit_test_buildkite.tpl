  - label: "{{.Name}}"
    commands:
        # Set up Go
        - ./.buildkite/setup_go.sh
        # Tune the OS
        - echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range
        # Get dependencies
        - export DEBIAN_FRONTEND="noninteractive"
        - sudo apt-get update
        {{if (eq .Platform "mysql80")}}
        # mysql57
        - sudo apt-get install -y mysql-server mysql-client
        {{else}}
        # !mysql57
        # Uninstall any previously installed MySQL first
        - ./.buildkite/uninstall_mysql.sh
        {{if (eq .Platform "mysql57")}}
        # mysql80
        - ./.buildkite/install_mysql80.sh
        {{end}}
        {{end}} {{/*outer if*/}}
        - ./.buildkite/install_dependencies.sh
        # Run make tools
        - make tools
        # Run test
        - eatmydata -- make unit_test
    timeout_in_minutes: 30
