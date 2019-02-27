package com.tpt.rohtspatil.kafka.tutorial1;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo1 {
  public static void main(String[] args) {
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty("bootstrap.servers", bootStrapServers);
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
  }
}
