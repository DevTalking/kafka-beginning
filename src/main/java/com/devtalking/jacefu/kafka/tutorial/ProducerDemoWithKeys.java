package com.devtalking.jacefu.kafka.tutorial;

import com.detalking.jacefu.kafka.KafkaConstant;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        // Create producer properties.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer.
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Create producer record with key.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KafkaConstant.FIRST_TOPIC, "This is the key","hello world!");

        // Send data - asynchronous
        kafkaProducer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic()  + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing: ", e);
                }
            }
        });

        // flush data and close producer.
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
