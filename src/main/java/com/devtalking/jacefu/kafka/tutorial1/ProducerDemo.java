package com.devtalking.jacefu.kafka.tutorial1;

import com.detalking.jacefu.kafka.KafkaConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Create producer properties.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer.
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Create producer record.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KafkaConstant.FIRST_TOPIC, "hello world!");

        // Send data - asynchronous
        kafkaProducer.send(producerRecord);

        // flush data and close producer.
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
