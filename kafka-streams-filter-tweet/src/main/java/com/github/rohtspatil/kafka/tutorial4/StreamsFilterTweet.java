package com.github.rohtspatil.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweet {

  private static JsonParser jsonParser = new JsonParser();

  private static Integer extractUserFollowersInTweet(String tweetJson) {
    try {
      return jsonParser
          .parse(tweetJson)
          .getAsJsonObject()
          .get("user")
          .getAsJsonObject()
          .get("followers_count")
          .getAsInt();
    } catch (NullPointerException e) {
      return 0;
    }
  }

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
    properties.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");

    KStream<String, String> filteredStream =
        inputTopic.filter((k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);
    filteredStream.to("important_tweets");

    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
    kafkaStreams.start();
  }
}
