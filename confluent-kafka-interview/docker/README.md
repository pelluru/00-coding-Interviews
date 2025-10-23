# Local Kafka Stack (Docker Compose)

Services:
- Zookeeper, Kafka broker, Schema Registry, Kafka Connect, ksqlDB Server, Control Center

## Usage
```bash
docker compose up -d
# Broker PLAINTEXT at localhost:29092
# Schema Registry at http://localhost:8081
# Connect at http://localhost:8083
# ksqlDB at http://localhost:8088
# Control Center at http://localhost:9021
```
Place any custom connector JARs into `connect-plugins/` before starting.
