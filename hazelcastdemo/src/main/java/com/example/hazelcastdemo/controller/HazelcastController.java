package com.example.hazelcastdemo.controller;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.hazelcastdemo.util.HazelUtil;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;

@RestController
public class HazelcastController {

	private static final Logger logger = LoggerFactory.getLogger(HazelcastController.class);
	
	@Autowired
	private HazelcastInstance hazelcastInstance;
	
	@GetMapping(path = "/test", produces = "text/html")
	public String testHazelcast(@RequestParam(value = "count", required = true) Integer count) {
		logger.info("Inside testHazelcast");
		
		Lock lock = hazelcastInstance.getCPSubsystem().getLock("mapClearLock");
		// Acquire the lock
		lock.lock();
		try {
			
		IMap<String, String> map = hazelcastInstance.getMap("DEMO_MAP");
		
		logger.info("Current map: " + map.size());
		String epoch = HazelUtil.getepoch();
		for (int i = 0; i < count; i++) {
//		    String key = "key_"+epoch + i;
			String key = "key_"+ i;
		    String value = "value_" + i;
		    map.put(key, value);
		}
		map.forEach((k,v)->{
			logger.info("Key: " + k + " Value: " + v);
		});
		
			// Clear the map
			map.clear();
			logger.info("Map is empty::"+map.isEmpty());
		} finally {
			// Release the lock
			lock.unlock();
		}
		
		return "DONE";
	}
	
	@GetMapping(path = "/hi")
	public void hi() {
		
		// Create a Hazelcast client configuration
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703");

        // Create a Hazelcast client instance
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        Set<Partition> partitions = client.getPartitionService().getPartitions();
        for(Partition partition: partitions) {
        	logger.info("Partition:" + partition+":Owner:"+partition.getOwner());
        }
        // Get the Hazelcast cluster
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        Collection<Member> members = clientProxy.getCluster().getMembers();

        // Print information about the cluster members
        logger.info("Number of members in the cluster: " + members.size());
        for (Member member : members) {
            logger.info("Member: " + member);
        }

        // Close the Hazelcast client
        client.shutdown();
	}
}
