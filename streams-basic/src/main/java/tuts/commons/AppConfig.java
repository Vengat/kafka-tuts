package tuts.commons;

/**
 * There are two topics that this producer publishes to
 */
public class AppConfig {

    public final static String applicationId = "KafkaProducer";
    public final static String bootstrapServers = "localhost:9092, localhost:9093";
    public static String topicName = "kafka_producer_topic";
    //public static String topicName1 = "kafka_producer_topic1";
    public static int numEvents = 10;
    //public final static String transaction_id = "Kafka_Producer_Transaction";

}
