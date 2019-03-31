package com.unisky.kafka.client;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by unisky on 2019/3/30.
 */
public class Commands {

    private static ControlEntityManager controlEntityManager = new ControlEntityManager();

    // 启动消费者
    public static void startConsumer(String topic, String name, boolean isReadCommit){
        if (controlEntityManager.isNameContain(name)){
            System.out.print(String.format("name: %s is already exists", name));
            return;
        }

        Map<String, Object> configMap = new HashMap<>();
        Config.consumerConfig.forEach((k, v)-> configMap.put(k,v));

        configMap.put("group.id", topic);

        if (isReadCommit){
            configMap.put("isolation.level", "read_committed");
        }

        ConsoleConsumer consoleConsumer = new ConsoleConsumer(configMap, new IntegerDeserializer(), new StringDeserializer());

        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic);
        consoleConsumer.subscribe(topics);

        ControlEntity controlEntity = new ControlEntity();
        controlEntity.setName(name);
        controlEntity.setConfig(configMap);
        controlEntity.setIsProducer(false);
        controlEntity.setConsumerOrProducer(consoleConsumer);
        controlEntity.setTopic(topic);

        ConsoleConsumerThread consoleConsumerThread = new ConsoleConsumerThread(consoleConsumer, controlEntity);
        controlEntityManager.addControlEntity(controlEntity);
        consoleConsumerThread.start();
    }

    // 启动带事务的生产者
    public static void startProducer(String name, String transactionId){
        if (controlEntityManager.isNameContain(name)){
            System.out.print(String.format("name: %s is already exists", name));
            return;
        }

        Map<String, Object> configMap = new HashMap<>();
        Config.producerConfig.forEach((k, v)-> configMap.put(k, v));

//        configMap.put("transaction.timeout.ms", "");
        if (transactionId != null){
            configMap.put("enable.idempotence", true);
            configMap.put("transactional.id", transactionId);
        }

        ConsoleProducer producer = new ConsoleProducer(configMap, new IntegerSerializer(), new StringSerializer());

        ControlEntity entity = new ControlEntity();
        entity.setName(name);
        entity.setIsProducer(true);
        entity.setConfig(configMap);
        entity.setConsumerOrProducer(producer);

        controlEntityManager.addControlEntity(entity);

        if (transactionId != null){
            producer.initTransactions();
        }
        System.out.println(String.format("init producer %s with transaction %s done", name, transactionId == null? "no": transactionId));
    }

    public static void sendMessage(String name, String topic, int partition , String message){
        if (name == null || message == null){
            System.out.println("name or message should not be null");
            return;
        }

        ControlEntity controlEntity = checkIfExist(name);
        if (controlEntity == null){
            return;
        }

        Object object = controlEntity.getConsumerOrProducer();
        if (object  instanceof  ConsoleConsumer){
            System.out.println(String.format("name %s is a consumer", name));
            return;
        }

        ConsoleProducer producer = (ConsoleProducer) object;

        ProducerRecord record = new ProducerRecord(topic, partition, partition, message);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null){
                    System.out.println("send done");
                } else {
                    exception.printStackTrace();
                }
            }
        });
    }

    public static void begin(String name){
        ControlEntity controlEntity = checkIfExist(name);
        if (controlEntity == null){
            return;
        }

        Object object = controlEntity.getConsumerOrProducer();
        if (object  instanceof  ConsoleConsumer){
            System.out.println(String.format("name %s is a consumer", name));
            return;
        }

        ConsoleProducer producer = (ConsoleProducer) object;
        producer.beginTransaction();
    }

    public static void commit(String name){
        ControlEntity controlEntity = checkIfExist(name);
        if (controlEntity == null){
            return;
        }

        Object object = controlEntity.getConsumerOrProducer();
        if (object  instanceof  ConsoleConsumer){
            System.out.println(String.format("name %s is a consumer", name));
            return;
        }

        ConsoleProducer producer = (ConsoleProducer) object;

        producer.commitTransaction();
    }

    public static void abort(String name){
        ControlEntity controlEntity = checkIfExist(name);
        if (controlEntity == null){
            return;
        }

        Object object = controlEntity.getConsumerOrProducer();
        if (object  instanceof  ConsoleConsumer){
            System.out.println(String.format("name %s is a consumer", name));
            return;
        }

        // TODO 记录当前producer的事务状态，用于实时查询
        ConsoleProducer producer = (ConsoleProducer) object;
        producer.abortTransaction();
    }

    // 删除某个生产者或消费者
    public static void deleteConsumerOrProducer(String name){
        ControlEntity controlEntity = checkIfExist(name);
        if (controlEntity == null){
            return;
        }

        Object object = controlEntity.getConsumerOrProducer();
        if (object  instanceof  ConsoleConsumer){
            controlEntity.setStop(true);
            ConsoleConsumer consumer = (ConsoleConsumer) object;
            consumer.close();
            System.out.println(String.format("close consumer %s...", name));
            return;
        }

        ConsoleProducer producer = (ConsoleProducer)object;
        producer.close();

        System.out.println(String.format("close producer %s...", name));
    }

    // 屏蔽所有收到的消息
    public static void muteAllReceivedMessage(){
        controlEntityManager.iterator().forEachRemaining(element->{
            ControlEntity entity = element.getValue();
            entity.setShowMessage(false);
        });
    }

    // 显示所有收到的消息
    public static void showAllReceivedMessage(){
        controlEntityManager.iterator().forEachRemaining(element->{
            ControlEntity entity = element.getValue();
            entity.setShowMessage(true);
        });
    }

    // 显示某个consumer的消息
    public static void showOneConsumerMessage(String name){
        ControlEntity entity = checkIfExist(name);
        if (entity == null){
            return;
        }

        entity.setShowMessage(true);
    }

    public static void muteOneConsumerMessage(String name){
        ControlEntity entity = checkIfExist(name);
        if (entity == null){
            return;
        }

        entity.setShowMessage(false);
    }

    // 当前实例信息
    public static void showAllInfo(){
        System.out.println("*********info****************************info****************");
        controlEntityManager.iterator().forEachRemaining(element->{
            ControlEntity entity = element.getValue();

            String format;
            if (!entity.isProducer()){
                format = String.format(
                        "name: %s\n" +
                        "type: consumer\n" +
                        "mute: %s\n" +
                        "topic: %s\n",

                        entity.getName(),
                        entity.isShowMessage() ? "false": "true",
                        entity.getTopic());
            } else {
                format = String.format(
                        "name: %s\n" +
                        "type: producer\n", entity.getName());
            }

            System.out.println(format);
        });
    }

    private static ControlEntity checkIfExist(String name){
        ControlEntity controlEntity = controlEntityManager.getControlEntity(name);
        if (controlEntity == null){
            System.out.println(String.format("no such name(%s) exist", name));
        }
        return controlEntity;
    }
}
