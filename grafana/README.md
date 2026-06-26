# Grafana Dashboard

This folder contains the Grafana dashboard used to monitor the LSMKV distributed cluster.

## Import

1. Open Grafana.
2. Go to Dashboards → Import.
3. Upload `dashboard.json`.

## Prometheus Data Source

Docker:

http://prometheus:9090

## Dashboard Metrics

- Active Connections
- MemTable Size
- MemTable Entries
- SSTable Count
- Bloom Filter Hit Rate
- Read Amplification
- Write Amplification