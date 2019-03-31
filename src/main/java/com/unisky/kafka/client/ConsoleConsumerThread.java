package com.unisky.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Iterator;

/**
 * Created by unisky on 2019/3/30.
 */
public class ConsoleConsumerThread extends Thread {

    private ConsoleConsumer consoleConsumer;
    private ControlEntity controlEntity;

    public ConsoleConsumerThread(ConsoleConsumer consoleConsumer, ControlEntity controlEntity){
        if (consoleConsumer == null || controlEntity == null){
            throw new IllegalArgumentException("consoleConsumer or controlEntity is null");
        }
        this.consoleConsumer = consoleConsumer;
        this.controlEntity = controlEntity;
    }

    @Override
    public void run() {
        Duration duration = Duration.ofMillis(1000);
        try {

            System.out.println("consumer "+ controlEntity.getName() + " start...");
            while (!controlEntity.isStop()){
                ConsumerRecords consumerRecords = consoleConsumer.poll(duration);
                if (consumerRecords == null || consumerRecords.isEmpty()){
                    continue;
                }

                Iterator<ConsumerRecord> iterator = consumerRecords.iterator();

                iterator.forEachRemaining(record -> {
                    try {
                        doProcess(record);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        System.out.println(String.format("consumer %s stop", controlEntity.getName()));
    }

    private void doProcess(ConsumerRecord record){
        if (!controlEntity.isShowMessage()){
            return;
        }

        String format = String.format("consumer: %s topic: %s partition: %d offset: %d message: %s",
                controlEntity.getName(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.value().toString());

        System.out.println(format);
    }
}
