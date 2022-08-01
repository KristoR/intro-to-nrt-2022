# creating topics - best practice to create them beforehand
# https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable

docker exec broker-3 \
kafka-topics --bootstrap-server broker-3:9092 \
--create \
--topic sales

docker exec broker-2 \
kafka-topics --bootstrap-server localhost:9092 \
--create \
--topic purchasing \
--replication-factor 3

docker exec broker-1 \
kafka-topics --bootstrap-server broker-2:9092 \
--create \
--topic wiki \
--partitions 3 

docker exec broker-1 \
kafka-topics --bootstrap-server localhost:9092 \
--create \
--topic logs \
--partitions 3 \
--replication-factor 3 

# view existing topics

docker exec broker-1 \
kafka-topics --bootstrap-server localhost:9092 \
--list

# view detailed info about topic

docker exec broker-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic logs

docker exec broker-1 \
kafka-topics --bootstrap-server localhost:9092 \
--describe --topic logs

# simple produce and consume
docker exec --interactive --tty broker-1 \
kafka-console-producer --bootstrap-server localhost:9092 \
                       --topic test

docker exec --interactive --tty broker-1 \
kafka-console-consumer --bootstrap-server localhost:9092 \
--topic test --from-beginning

# Schema registry
# https://docs.confluent.io/platform/current/schema-registry/develop/using.html#

# consume avro messages with key as string
docker exec --interactive --tty schema-registry kafka-avro-console-consumer --bootstrap-server broker-1:9092  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --topic sales3 --from-beginning

# topic _schemas for schema registry schemas
