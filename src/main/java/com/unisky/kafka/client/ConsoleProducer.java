package com.unisky.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by unisky on 2019/3/30.
 */
public class ConsoleProducer extends KafkaProducer<Integer, String>{

    public ConsoleProducer(Map<String, Object> configs, Serializer<Integer> keySerializer, Serializer<String> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }
}
