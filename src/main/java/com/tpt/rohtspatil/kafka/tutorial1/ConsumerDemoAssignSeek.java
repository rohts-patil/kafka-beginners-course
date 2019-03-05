package com.tpt.rohtspatil.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

  public static void main(String[] args) {

    String bootStrapServers = "127.0.0.1:9092";
    String topic = "first_topic";

    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    // create consumer config
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // assign and seek used to replay a data or fetch a specific message
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);

    long offSetToReadFrom = 15L;

    consumer.assign(Arrays.asList(partitionToReadFrom));

    consumer.seek(partitionToReadFrom, offSetToReadFrom);

    int numOfMessagesToRead = 5;
    int numOfMessagesReadSoFar = 0;
    boolean keepOnReading = true;

    // poll for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord record : records) {
        numOfMessagesReadSoFar += 1;
        logger.info("key:" + record.key() + " value:" + record.value());
        logger.info("partition:" + record.partition() + " offset:" + record.offset());
        if (numOfMessagesReadSoFar >= numOfMessagesToRead) {
          keepOnReading = false;
          break;
        }
      }
    }
    logger.info("Exiting the application.");
  }
}
