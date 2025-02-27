package com.example.hazelcastdemo.config;

import com.hazelcast.partition.PartitioningStrategy;

public class CustomPartitioningStrategy implements PartitioningStrategy<Object> {

	private static final long serialVersionUID = 1L;

	@Override
    public Object getPartitionKey(Object keyData) {
        Object key = keyDataToObject(keyData);
        Object partitionKey = generatePartitionKey(key);
//        System.out.println("Key: " + keyData);
//        System.out.println("partitionKey: " + partitionKey);
        return partitionKey;
    }

    private Object keyDataToObject(Object keyData) {
        return keyData != null ? keyData.toString() : null;
	}

	private Object generatePartitionKey(Object key) {
        return key != null ? key.hashCode() : null;
    }

}
