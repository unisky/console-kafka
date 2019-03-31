package com.unisky.kafka.client;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by unisky on 2019/3/30.
 */
public class Config {

    public static final String HELP =
            "start consumer name_readCommit topic A  rc\n" +
            "start consumer name_readUncommit topic A ru\n" +
//            "# start consumer name_readCommit topic A partition 1,2,3 rc\n" +
//            "# start consumer name_readUnCommit topic A partition 1,2,3 ru\n" +
            "start producer name transaction_id\n" +
            "start producer name\n" +
            "close name(consumer or producer)\n" +
            "send producerName topic A(if no, use first) partition 1 message\n" +
            "send producerName begin\n" +
            "send producerName) commit\n" +
            "send producerName abort\n" +
            "show all\n" +
            "show consumerName\n" +
            "mute all\n" +
            "mute consumeName\n" +
            "info\n" +
            "help";

    private static final String BROKER_SERVER = "127.0.0.1:9091;127.0.0.1:9092";
    private static final String KEY_SERIALIZATION = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String VALUE_SERIALIZATION = "org.apache.kafka.common.serialization.StringSerializer";

    public static Map<String, String> producerConfig = new HashMap<>();
    public static Map<String, String> consumerConfig = new HashMap<>();

    static {
        producerConfig.put("bootstrap.servers", BROKER_SERVER);
//        producerConfig.put("transaction.timeout.ms", "60000");
        producerConfig.put("key.serializer", KEY_SERIALIZATION);
        producerConfig.put("value.serializer", VALUE_SERIALIZATION);
    }

    static {
        consumerConfig.put("bootstrap.servers", BROKER_SERVER);
        consumerConfig.put("transaction.timeout.ms", "60000");
        consumerConfig.put("key.deserializer", KEY_SERIALIZATION);
        consumerConfig.put("value.deserializer", VALUE_SERIALIZATION);
    }



}
