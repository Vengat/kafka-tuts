package com.vengat.tuts;

import com.vengat.tuts.commons.AppConfig;
import com.vengat.tuts.serde.JsonDeserializer;
import com.vengat.tuts.serde.JsonSerializer;
import com.vengat.tuts.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTuts {

    private static final Logger logger = LogManager.getLogger(KafkaProducerTuts.class);

    public static void main(String[] args) {

        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(AppConfig.kafkaConfigLocation);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
        try {
            consumerProperties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProperties.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.groupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(consumerProperties);
        consumer.subscribe(Arrays.asList(AppConfig.topicName1, AppConfig.topicName1));


        logger.info("creating kafka producer");

        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> kafkaProducer = new KafkaProducer<String, PosInvoice>(properties);


        while(true) {
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, PosInvoice> record : records) {
                if(record.value().getDeliveryType().equalsIgnoreCase("HOME-DELIVERY") && record.value().getDeliveryAddress().getContactNumber().equals("")) {
                    kafkaProducer.send(new ProducerRecord<>(AppConfig.invalidTopic, record.value().getStoreID(), record.value()));
                    logger.info("invalid record - " +record.value().getInvoiceNumber());
                } else {
                    kafkaProducer.send(new ProducerRecord<>(AppConfig.validTopic, record.value().getStoreID(), record.value()));
                    logger.info("valid record - " +record.value().getInvoiceNumber());
                }
            }
        }


        producer.close();
    }

}
