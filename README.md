# kafka
Started working with Kafka

If you want to run this program, then run zookeeper first by command:
 bin/windows/zookeeper-server-start.bat config/zookeeper.properties
 
 Then run kafka server :
 bin/windows/kafka-server-start.bat config/server.properties

Create topic:
bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <name_of_topic>

Producer side:
bin/windows/kafka-console-producer.bat --broker-list localhost:9092 --topic <name_of_your_topic>

Consumer side:
bin/windows/kafka-console-consumer.bat --zookeeper localhost:2181 --topic test

Have fun!
