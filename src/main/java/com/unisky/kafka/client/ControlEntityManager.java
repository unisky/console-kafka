package com.unisky.kafka.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by unisky on 2019/3/30.
 */
public class ControlEntityManager {

    private Map<String, ControlEntity> controlEntityMap = new HashMap<>();

    public boolean isNameContain(String name){
        return controlEntityMap.containsKey(name);
    }

    public void addControlEntity(ControlEntity controlEntity){
        if(controlEntityMap.containsKey(controlEntity.getName())){
            throw new RuntimeException("this name has already inside, please remove that first, name: "+ controlEntity.getName());
        }

        controlEntityMap.put(controlEntity.getName(), controlEntity);

    }

    public ControlEntity getControlEntity(String name){
        return controlEntityMap.get(name);
    }

    public Iterator<Map.Entry<String, ControlEntity>> iterator(){
        return controlEntityMap.entrySet().iterator();
    }

}
