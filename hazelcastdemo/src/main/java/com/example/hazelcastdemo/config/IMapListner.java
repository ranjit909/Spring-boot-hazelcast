package com.example.hazelcastdemo.config;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryAddedListener;

public class IMapListner implements EntryAddedListener<String, String>{
	
	private static final Logger logger = LoggerFactory.getLogger(IMapListner.class);
	
	private final HazelcastInstance hazelcastInstance;
	
	private int count = 0;
	 
    public IMapListner(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.count = 0;
    }

    @Override
    public void entryAdded(EntryEvent<String, String> event) {
        // Process the added entry here
    	UUID instanceUuid = hazelcastInstance.getLocalEndpoint().getUuid();
    	UUID ownerInstanceUuid = hazelcastInstance.getPartitionService().getPartition(event.getKey()).getOwner().getUuid(); 
//    	int ownerInstanceId = ownerInstanceUuid.hashCode();
    	logger.info("instanceUuid:"+instanceUuid+" ownerInstanceUuid:"+ownerInstanceUuid);
//        if(instanceUuid==ownerInstanceId) {
    	 if(instanceUuid==ownerInstanceUuid) {
	    	String key = event.getKey();
	        String value = event.getValue();
	        logger.info("Entry added: Key = " + key + ", Value = " + value);
	        try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        count++;
	        logger.info("Total elements: " + count);
        }
    }

}
