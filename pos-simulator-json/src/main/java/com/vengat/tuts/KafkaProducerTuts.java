package com.vengat.tuts;

import com.vengat.tuts.commons.AppConfig;
import com.vengat.tuts.datagenerator.ProductGenerator;
import com.vengat.tuts.serde.JsonSerializer;
import com.vengat.tuts.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTuts {

    private static final Logger logger = LogManager.getLogger(KafkaProducerTuts.class);

    public static void main(String[] args) {

        logger.info("creating kafka producer");

        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(AppConfig.kafkaConfigLocation);
        } catch (FileNotFoundException e) {
           throw new RuntimeException(e);
        }
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
          //properties.put();

        //Thread[] dispatechers = new Thread[AppConfig.eventFiles.length];
        //KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);


//        logger.info("Start sending messages");
//
//        for(int i = 0; i <= AppConfig.eventFiles.length; i++) {
//            dispatechers[i] = new Thread(new RunnableProducer(producer, AppConfig.topicName1, AppConfig.eventFiles[i]));
//            dispatechers[i].start();
//        }
//
//        for(Thread t : dispatechers) {
//            try {
//                t.join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(properties);

        int noOfProducers = 10;
        int produceSpeed = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(noOfProducers);
        List<RunnablePosProducer> runnablePosProducers = new ArrayList<>(noOfProducers);

        for(int i = 0; i < noOfProducers; i++) {
            RunnablePosProducer runnablePosProducer = new RunnablePosProducer(i, producer, AppConfig.topicName1, produceSpeed);
            runnablePosProducers.add(runnablePosProducer);
            executorService.submit(runnablePosProducer);
        }

        //logger.info("Finished - closing kafka producer");
        //producer.close();

        Runtime.getRuntime().addShutdownHook( new Thread(() -> {
            for(RunnablePosProducer p : runnablePosProducers) p.shutdown();
            executorService.shutdown();
            logger.info("Closing Executor Service");

            try {
                executorService.awaitTermination(produceSpeed * 2, TimeUnit.MILLISECONDS);
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }

        }));

        producer.close();
    }

}
