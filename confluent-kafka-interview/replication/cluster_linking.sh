# Cluster Linking (Confluent Cloud) — conceptual CLI steps
# Prereq: Two Confluent Cloud clusters: source and destination
# 1) On destination cluster, create a link to the source:
confluent kafka link create dr-link \
  --source-bootstrap-server <SRC_BOOTSTRAP> \
  --source-cluster-id <SRC_CLUSTER_ID> \
  --config security.protocol=SASL_SSL \
  --config sasl.mechanism=PLAIN \
  --config sasl.username=<SRC_API_KEY> \
  --config sasl.password=<SRC_API_SECRET>

# 2) Mirror a topic
confluent kafka mirror create orders --link dr-link

# 3) Check status
confluent kafka mirror list --link dr-link
