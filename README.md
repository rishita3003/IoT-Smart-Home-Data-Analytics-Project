Used the dataset of https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data and https://www.kaggle.com/datasets/rober2598/madrid-weather-dataset-by-hours-20192022

If Someone wants to setup Kafka for realtime data stream, then follow the steps below -: 
1. Install Kafka from https://kafka.apache.org/downloads
I downloaded https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz 
2. Edit environment variables and add the path "C:\Kafka\kafka_2.13–3.8.0\bin\windows" in the path section
3. Click ok, ok, ok
4. Open path: C:\Kafka\kafka_2.13–3.8.0\config and find Server.properties and zookeeper.properties
5. Find and edit the log.dirs attribute inside the Server.properties like below correspond to your path
logs.dir = "C:/Kafka/kafka_2.13-3.8.0/kafka-logs"
6. Find and edit the dataDir attribute inside the zookeeper.properties like below correspond to your path
dataDir = "C:/Kafka/kafka_2.13-3.8.0/zookeeper-data"
7. Open a command prompt and navigate to your ZooKeeper installation directory 
cd "C:\Kafka\kafka_2.13-3.8.0\bin\windows"
8. Type the command line below in the command prompt above (inside ZooKeeper installation directory) and Enter
zookeeper-server-start.bat ..\..\config\zookeeper.properties
9. Open a command prompt and navigate to your kafka-server-start.bat installation directory
cd "C:\Kafka\kafka_2.13-3.8.0\bin\windows"
10. Type the command line below in the command prompt above (inside Kafka installation directory) and Enter
kafka-server-start.bat ..\..\config\server.properties
11. A. Open another command prompt and type
kafka-topics.bat --bootstrap-server localhost:9092 --topic temperature --create --partitions 3 --replication-factor 1
kafka-topics.bat --create --topic raw-climate-data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

## Produce Messages
12. Open another command prompt and type
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic temperature
## Consumer Messages
13. Open another command prompt and type
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic temperature
## Testing Streaming data
A. Type the message in the Produce Messages command prompt open in step 12
B. You will see the message in the Consumer command prompt that we open in step 13

## To list all topics, open command prompt type
kafka-topics.bat --bootstrap-server localhost:9092 --list




