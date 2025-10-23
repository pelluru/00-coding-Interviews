# Kafka CLI Cheat-Sheet

```bash
# Create a topic
kafka-topics --create --topic orders --partitions 6 --replication-factor 3 --bootstrap-server $BS

# List / describe
kafka-topics --list --bootstrap-server $BS
kafka-topics --describe --topic orders --bootstrap-server $BS

# Produce JSON
kafka-console-producer --topic orders --bootstrap-server $BS \
  --property parse.key=true --property key.separator=:

# Consume (earliest)
kafka-console-consumer --topic orders --bootstrap-server $BS \
  --from-beginning --group demo-g1 --property print.key=true --property key.separator=:

# View consumer groups
kafka-consumer-groups --bootstrap-server $BS --list
kafka-consumer-groups --bootstrap-server $BS --describe --group demo-g1
```

