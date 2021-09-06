package com.vengat.tuts.commons;

public class AppConfig {

    public final static String applicationId = "pos_simulator-json";
    public static final String kafkaConfigLocation = "kafka.properties";
    //public final static String bootstrapServers = "localhost:9092, localhost:9093";
    public final static String topicName1 = "pos-simulator-json-1";
    public final static String topicName2 = "pos-simulator-json-2";
    public static int numEvents = 10;
    public final static String[] eventFiles = {"data/NSE05NOV2018BHAV.csv","data/NSE06NOV2018BHAV.csv"};

}
