# RBAC & API key workflow (Confluent Cloud) using confluent CLI
# Login & set env/cluster first:
# confluent login
# confluent environment use <env-id>
# confluent kafka cluster use <cluster-id>

# 1) Create a service account
confluent iam service-account create app-svc --description "Kafka app service account"

# 2) Grant role(s) at the cluster level (e.g., DeveloperWrite on topic 'orders')
confluent kafka acl create --allow --service-account <svc-id> --operation WRITE --topic orders
confluent kafka acl create --allow --service-account <svc-id> --operation READ --consumer-group app-group

# 3) Create API key scoped to the Kafka cluster for that service account
confluent api-key create --resource <cluster-id> --service-account <svc-id>
# Note the API Key and Secret => use in client config

# 4) Verify ACLs
confluent kafka acl list --service-account <svc-id>
