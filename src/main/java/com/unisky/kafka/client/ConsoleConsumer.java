package com.unisky.kafka.client;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by unisky on 2019/3/30.
 */
public class ConsoleConsumer extends KafkaConsumer<Integer, String>{
    public ConsoleConsumer(Map<String, Object> configs, Deserializer keyDeserializer, Deserializer valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }
}
