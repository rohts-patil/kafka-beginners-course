package kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

  public static void main(String[] args) {

    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    String bootStrapServers = "127.0.0.1:9092";

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      // create producer record

      String topic = "first_topic";
      String key = "id_" + Integer.toString(i);
      String value = "Hello World" + Integer.toString(i);
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      // send data async
      producer.send(
          record,
          new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time record is successfully sent or an exception is thrown
              if (e == null) {
                logger.info(
                    "Received new metadata. \n"
                        + "Topic: "
                        + recordMetadata.topic()
                        + "\n"
                        + "Partition: "
                        + recordMetadata.partition()
                        + "\n"
                        + "Offset: "
                        + recordMetadata.offset()
                        + "\n"
                        + "TimeStamp: "
                        + recordMetadata.timestamp());
              } else {
                logger.error("Error while producing ", e);
              }
            }
          });
    }
    // flush data
    producer.flush();
    producer.close();
  }
}
