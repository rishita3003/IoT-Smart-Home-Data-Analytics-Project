# Kafka Real-Time Data Streaming Setup

This guide describes the setup process for Kafka to handle real-time data streaming. It is tailored for Windows environments and uses datasets from Kaggle to demonstrate Kafka's capabilities. The datasets used are:
- [Daily Climate Time Series Data](https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data)
- [Madrid Weather Dataset by Hours (2019-2022)](https://www.kaggle.com/datasets/rober2598/madrid-weather-dataset-by-hours-20192022)

## Prerequisites

Before you begin, ensure you have downloaded and extracted Kafka. For this setup, the Kafka version used is `3.8.0` for Scala `2.13`.

Download Kafka: [Apache Kafka 3.8.0](https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz)

## Installation

1. **Install Kafka**:
   - Download Kafka from [Apache Kafka Downloads](https://kafka.apache.org/downloads).
   - Extract the downloaded file to a preferred location, e.g., `C:\Kafka\`.

2. **Set Environment Variables**:
   - Add Kafka's `bin\windows` directory to your system's PATH to allow running Kafka commands from any command prompt.
     ```
     Path: C:\Kafka\kafka_2.13-3.8.0\bin\windows
     ```

3. **Install Java JDK 11**:
   - Download and install Java JDK 11 from [Oracle JDK 11 Downloads](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html).

4. **Set Java Environment Variables**:
   - Add the following paths to your system's PATH variable:
     ```
     C:\Program Files\Java\jdk-11.0.24\bin
     C:\Program Files\Java
     ```
   - Set the `JAVA_HOME` variable in the system variables section:
     ```
     JAVA_HOME = C:\Program Files\Java\jdk-11.0.24
     ```

5. **Configure Kafka and ZooKeeper**:
   - Navigate to `C:\Kafka\kafka_2.13-3.8.0\config`.
   - Edit `server.properties` and set the `log.dirs` to point to the Kafka logs directory:
     ```
     log.dirs=C:/Kafka/kafka_2.13-3.8.0/kafka-logs
     ```
   - Edit `zookeeper.properties` and set the `dataDir` to point to the ZooKeeper data directory:
     ```
     dataDir=C:/Kafka/kafka_2.13-3.8.0/zookeeper-data
     ```

## Running Kafka

1. **Start ZooKeeper**:
   - Open a command prompt and navigate to your Kafka installation's `bin\windows` directory:
     ```
     cd C:\Kafka\kafka_2.13-3.8.0\bin\windows
     ```
   - Start ZooKeeper using the following command:
     ```
     zookeeper-server-start.bat ..\..\config\zookeeper.properties
     ```

2. **Start Kafka Server**:
   - Open a new command prompt as described above and start the Kafka server:
     ```
     kafka-server-start.bat ..\..\config\server.properties
     ```

## Kafka Topics Management

1. **Create Topics**:
   - To create topics, open a new command prompt and type:
     ```
     kafka-topics.bat --create --topic temperature --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
     kafka-topics.bat --create --topic raw-climate-data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
     ```

2. **List Topics**:
   - To list all existing topics:
     ```
     kafka-topics.bat --bootstrap-server localhost:9092 --list
     ```

## Producing and Consuming Messages

1. **Produce Messages**:
   - Open a new command prompt and run the producer:
     ```
     kafka-console-producer.bat --bootstrap-server localhost:9092 --topic temperature
     ```

2. **Consume Messages**:
   - Open another command prompt and start the consumer:
     ```
     kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic temperature
     ```

   - Type a message in the producer command prompt and observe it appearing in the consumer prompt, demonstrating real-time data streaming.

## Conclusion

This README outlines the steps required to set up Kafka for real-time data streaming on Windows. For real-world applications, consider securing your Kafka deployment and managing it for performance and reliability.
