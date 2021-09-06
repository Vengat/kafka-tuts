package com.vengat.tuts;

import com.vengat.tuts.datagenerator.InvoiceGenerator;
import com.vengat.tuts.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnablePosProducer implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private KafkaProducer<String, PosInvoice> producer;
    private String topicName;
    private AtomicBoolean stopper = new AtomicBoolean(false);
    private InvoiceGenerator invoiceGenerator;
    private int produceSpeed;
    private int id;

    public RunnablePosProducer(int id, KafkaProducer<String, PosInvoice> producer, String topicName, int produceSpeed) {
        this.id = id;
        this.producer = producer;
        this.topicName = topicName;
        this.produceSpeed = produceSpeed;
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }

    @Override
    public void run() {
        try {
            logger.info("Starting producer thread " + id);
            while(!stopper.get()) {
                PosInvoice posInvoice = invoiceGenerator.getNextInvoice();
                producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID(), posInvoice));
                Thread.sleep(produceSpeed);
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void shutdown() {
        logger.info("Shutting down producer thread " + id);
        stopper.set(true);
    }
}
