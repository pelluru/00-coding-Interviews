# Confluent Cloud: Secure Clients (API Keys, RBAC)

This folder shows **secure client configs** for Confluent Cloud using **SASL_SSL + PLAIN** with API Key/Secret,
and examples of **RBAC** via the `confluent` CLI.

## Java client.properties
- Set `security.protocol=SASL_SSL`
- Use `sasl.mechanism=PLAIN`
- Provide API Key/Secret in `sasl.jaas.config`

> Rotate keys regularly; use **Service Accounts** (not user keys) for apps.

## Python (confluent_kafka) config
- Same security settings (`security.protocol`, `sasl.mechanism`, username/password)

## RBAC (high level)
- Create a service account
- Grant roles at cluster/topic scope (DeveloperRead/Write, ResourceOwner)
- Create API key scoped to the Kafka cluster
- Use the key in your client config
