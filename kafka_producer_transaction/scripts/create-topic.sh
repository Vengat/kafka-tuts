sh kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 5 --topic kafka_producer_topic  --config min.insync.replicas=2