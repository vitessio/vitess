.PHONY: dashboards_out prometheus_alerts.yaml prometheus_rules.yaml test tools all e2e e2e-dev e2e-compose-up e2e-compose-down lint fmt

help: #: Show this message.
	@echo "\nAvailable Targets:\n"
	@sed -ne '/@sed/!s/#: //p' $(MAKEFILE_LIST)

all: #: format all .jsonnet/.libsonnet files, generate all dashboards json, alerts and rules yaml
	@make tools 
	@make clean > /dev/null
	@make fmt > /dev/null
	@make test
	@make dashboards_out > /dev/null
	@make prometheus_rules.yaml > /dev/null

fmt: #: Usage make fmt
	@echo "# Formatting all .libsonnet and .jsonnet files...\n"
	@scripts/fmt.sh
	@echo "\nDone!\n"

a.yaml: #: Build prometheus alerts
	@echo "# Building 'prometheus_alerts.yaml'...\n"
	@jsonnet -S lib/alerts.jsonnet > $@
	@echo "\nDone!\n"

prometheus_rules.yaml: #: Build prometheus rules
	@echo "# Building 'prometheus_rules.yaml'...\n"
	@jsonnet -S lib/rules.jsonnet > $@
	@echo "\nDone!\n"

dashboards_out: #: Generate Grafana Dashboards Usage: `ENV='prod' make dashboards_out` 
	@echo "# Building Grafana dashboards...\n"
	@make clean > /dev/null
	@mkdir -p dashboards_out
	@[ "${ENV}" = 'dev' ] || [ "${ENV}" = 'prod' ] || ( echo -e "##ERROR\nPlease specify ENV (prod or dev)"; exit 1)
	@jsonnet -J vendor --ext-str env="${ENV}" -m dashboards_out lib/dashboards.jsonnet 
	@echo "\nDone!\n"

lint: #: Usage: make lint
	@echo "# Linting all .libsonnet and .jsonnet files...\n"
	@scripts/fmt.sh --check
	@echo "\nDone!\n"
	@echo "# Linting 'prometheus_rules.yaml'...\n"
	@make prometheus_rules.yaml > /dev/null
	@promtool check rules prometheus_rules.yaml
	@echo "Done!\n"

clean: #: Delete generated dashboards (/dashboards_out) Usage: make clean 
	@echo "# Cleaning up all generated files...\n"
	@rm -rf dashboards_out prometheus_alerts.yaml prometheus_rules.yaml > /dev/null
	@echo "\nDone!\n"

test: #: Compare your .json generated dashboards local version with the origin/main version.
	@make clean > /dev/null
	@ENV=${ENV} scripts/vitess-mixin-plan.sh
	@make clean > /dev/null

tools: tools.go
	@# -mod='' tells go to ignore the vendor/ directory
	@cat $^ | grep _ | awk -F'"' '{print $$2}' | xargs -I% go install -mod='' %
	@jb install 
	@jb update https://github.com/grafana/grafonnet-lib/grafonnet > /dev/null 2>&1


E2E_GRAFANA_VERSION ?= 7.3.6

e2e: #: Run all Grafana end-to-end tests.
	GRAFANA_VERSION=${E2E_GRAFANA_VERSION} \
	docker-compose -f e2e/docker-compose.yml up \
		--abort-on-container-exit \
		--exit-code-from e2e \
		--remove-orphans

e2e-dev: #: Run Grafana e2e tests in Cypress test runner.
	GRAFANA_VERSION=${E2E_GRAFANA_VERSION} \
	DISPLAY=$$(ipconfig getifaddr en0):0 \
	docker-compose -f e2e/docker-compose.dev.yml up \
		--abort-on-container-exit \
		--exit-code-from e2e \
		--remove-orphans

e2e-compose-up: #: Run Grafana e2e environment  spining Prometheus and Vitess in the backend.
	GRAFANA_VERSION=${E2E_GRAFANA_VERSION} \
	COMPOSE_HTTP_TIMEOUT=200 \
	DISPLAY=$$(ipconfig getifaddr en0):0 \
	docker-compose -f e2e/docker-compose.vt.yml up \
		--remove-orphans

e2e-compose-down: #: Clean docker compose resources
	@docker-compose -f e2e/docker-compose.vt.yml down -v --remove-orphans 
	@rm e2e/grafana/provisioning/dashboards/*.json
	@rm e2e/prometheus/prometheus_rules.yaml
