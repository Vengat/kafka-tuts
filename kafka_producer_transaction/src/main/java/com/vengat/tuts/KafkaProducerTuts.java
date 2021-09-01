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

/**
 * This producer can also be used to produce for a single topic
 */

public class KafkaProducerTuts {

    private static final Logger logger = LogManager.getLogger(KafkaProducerTuts.class);

    public static void main(String[] args) {

        logger.info("Creating kafka producer");

        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfig.transaction_id);

        org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        producer.initTransactions();

        
        logger.info("start sending messages - First Transaction");
        producer.beginTransaction();
        try {
            for(int i = 1; i <=AppConfig.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message T1 " + i));
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message T1 " + i));
            }
            logger.info("Committing First transaction");
            producer.commitTransaction();
        } catch(Exception e) {
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("start sending messages - Second Transaction");
        producer.beginTransaction();
        try {
            for(int i = 1; i <=AppConfig.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message T2 " + i));
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message T2 " + i));
            }
            logger.info("Committing Second transaction");
            producer.abortTransaction();
        } catch(Exception e) {
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }


        logger.info("Finished - Closing Kafka Producer");
        producer.close();
    }
}
