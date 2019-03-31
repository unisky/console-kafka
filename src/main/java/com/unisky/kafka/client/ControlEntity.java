package com.unisky.kafka.client;

import java.util.Map;

/**
 * Created by unisky on 2019/3/30.
 */
public class ControlEntity {

    private boolean isShowMessage = false;
    private Map<String, Object> config;
    private String name;
    private boolean isProducer;
    private boolean isStop = false;
    private String topic;
    private Object consumerOrProducer;

    public boolean isShowMessage() {
        return isShowMessage;
    }

    public void setShowMessage(boolean showMessage) {
        isShowMessage = showMessage;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isProducer() {
        return isProducer;
    }

    public void setIsProducer(boolean producer) {
        isProducer = producer;
    }

    public boolean isStop() {
        return isStop;
    }

    public void setStop(boolean stop) {
        isStop = stop;
    }

    public void setConsumerOrProducer(Object consumerOrProducer) {
        this.consumerOrProducer = consumerOrProducer;
    }

    public Object getConsumerOrProducer() {
        return consumerOrProducer;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
