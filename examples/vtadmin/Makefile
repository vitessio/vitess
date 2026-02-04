# Makefile for VTAdmin demo cluster

.PHONY: up down restart logs clean reset

# Base command with all required files and environment variables
COMPOSE_CMD = docker compose -f ../compose/docker-compose.yml -f docker-compose.yml --env-file ../compose/template.env

# Start the cluster
up:
	$(COMPOSE_CMD) up -d --force-recreate

# Stop the cluster
down:
	$(COMPOSE_CMD) down --remove-orphans

# Restart running services
restart:
	$(COMPOSE_CMD) restart

# Perform a full reset (down with volume cleanup, then up)
reset:
	$(COMPOSE_CMD) down --remove-orphans
	$(COMPOSE_CMD) up -d --force-recreate

# Stream logs from all services
logs:
	$(COMPOSE_CMD) logs -f

# Clean up all resources including volumes
clean:
	$(COMPOSE_CMD) down -v --remove-orphans
