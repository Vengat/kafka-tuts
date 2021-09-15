package tuts;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.Logger;
import tuts.commons.AppConfig;

import java.util.Properties;

public class HelloStreams {

    private static final Logger logger =

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, String> kStream = streamsBuilder.stream(AppConfig.topicName);
        kStream.foreach((k, v) -> System.out.println("Keys = " + k + " Value " + v));
        //kStream.peek((k, v) -> System.out.println("Keys = " + k + " Value " + v));
        logger.info("starting streams");

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("shutting down stream");
                    streams.close();
        })
        );

    }
}
