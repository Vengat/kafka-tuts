package com.vengat.tuts;

import com.vengat.tuts.commons.AppConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;


public class KafkaProducerTuts {

    private static final Logger logger = LogManager.getLogger(KafkaProducerTuts.class);

    public static void main(String[] args) {

        logger.info("Creating kafka producer");

        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        
        logger.info("start sending messages");

        for(int i = 1; i <=AppConfig.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfig.topicName, i, "Simple Message " + i));
        }

        logger.info("Finished - Closing Kafka Producer");
        producer.close();
    }
}
