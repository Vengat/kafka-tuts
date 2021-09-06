package com.vengat.tuts;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class RunnableProducer implements Runnable {

    private Logger logger = LogManager.getLogger(RunnableProducer.class);

    private KafkaProducer<Integer, String> producer;
    private String topicName;
    private String fileLocation;

    public RunnableProducer(KafkaProducer<Integer, String> producer, String topicName, String fileLocation) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }

    @Override
    public void run() {

        File file = new File(fileLocation);
        int counter = 0;

        try (Scanner scanner = new Scanner(file)) {
            while(scanner.hasNext()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
            }

            counter++;
            logger.info("Sent " + counter + "messages from " + fileLocation);
        } catch(FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
}
