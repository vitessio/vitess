# VTAdmin Demo

This example provides a fully functional local Vitess cluster with **VTAdmin**, **Grafana**, and **Percona Monitoring and Management (PMM)** pre-configured and integrated.

It demonstrates how VTAdmin can serve as a single pane of glass for your database infrastructure, providing context-aware deep links to your monitoring dashboards.

## Quick Start

1. **Start the cluster**:
   ```bash
   cd examples/vtadmin
   make up
   ```
2. **Access VTAdmin**:
   Open **http://localhost:5173** in your browser.

## Features

- **Unified Interface**: View cluster topology, tablet health, and gates.
- **Integrated Monitoring**:
    - **Vitess Metrics**: Deep links to Grafana dashboards for clusters, tablets, and gates.
    - **MySQL Metrics**: Deep links to PMM for database instance analysis.
- **Pre-configured Stack**: Includes Prometheus, Grafana, and PMM running alongside Vitess.

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| **VTAdmin** | http://localhost:5173 | - |
| **Grafana** | http://localhost:3000 | default |
| **PMM** | http://localhost:8888 | default |
| **Prometheus** | http://localhost:9090 | - |

## Configuration

The dashboard links in VTAdmin are configured via environment variables in `docker-compose.yml`. You can modify these to point to your own external monitoring infrastructure:

```yaml
vtadmin-web:
  environment:
    VITE_VITESS_MONITORING_CLUSTER_TEMPLATE: "http://your-grafana/..."
    VITE_MYSQL_MONITORING_TEMPLATE: "http://your-pmm/..."
```

See `web/vtadmin/README.md` for all available environment variables.

## Common Commands

### Cluster Control
```bash
make up       # Start cluster with current configuration
make restart  # Restart all services
make reset    # Full teardown and fresh start (removes volumes)
make down     # Stop all services
make clean    # Stop and remove all data volumes
make logs     # Stream logs from all services
```

