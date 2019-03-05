package com.tpt.rohtspatil.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private ConsumerDemoWithThread() {}

  private void run() {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    String bootStrapServers = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first_topic";
    CountDownLatch latch = new CountDownLatch(1);
    logger.info("Creating the consumer thread");
    Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootStrapServers, groupId, topic);

    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // add a shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Caught Shutdown hook");
                  ((ConsumerRunnable) myConsumerRunnable).shutDown();
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  logger.info("Application has exited.");
                }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted", e);
    } finally {
      logger.info("Application is closing.");
    }
  }

  public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;

    private KafkaConsumer<String, String> consumer;

    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    public ConsumerRunnable(
        CountDownLatch latch, String bootStrapServers, String groupId, String topic) {
      this.latch = latch;

      // create consumer config
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);
      // subscribe consumer to topics
      consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
      try {
        // poll for new data
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord record : records) {
            logger.info("key:" + record.key() + " value:" + record.value());
            logger.info("partition:" + record.partition() + " offset:" + record.offset());
          }
        }
      } catch (WakeupException w) {
        logger.info("Received shutdown signal");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutDown() {
      // special method to interrupt consumer.poll will throw wakeUpException
      consumer.wakeup();
    }
  }
}
