# Kafka-Stream-PaymentStatusApp


## Setup
Payment topic creation
```./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic payment --create --partitions 3 --replication-factor 1```
Payment Status topic creation
```./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic payment_status --create --partitions 3 --replication-factor 1 \
--config cleanup.policy=compact \
--config delete.retention.ms=300000
```

## Monitoring
Payment status consumer
```
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--topic payment_status
```

Payment consumer
```
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--topic payment
```
