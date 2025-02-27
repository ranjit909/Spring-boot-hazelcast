package com.example.hazelcastdemo.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.example.hazelcastdemo.controller.HazelcastController;
import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

@Configuration
@Component
public class HazelcastConfig {

	private static final Logger logger = LoggerFactory.getLogger(HazelcastController.class);
	
	@Autowired
	private Environment environment;
	
	@Bean
	public HazelcastInstance getInstance() {
		Config config = new Config();

        // Enable multicast and TCP/IP join mechanisms
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
     // Add IP addresses and ports of cluster members
        logger.info(environment.getProperty("ApplicationInstance"));
        List<String> list = Arrays.asList(environment.getProperty("ApplicationInstance").split(","));
        logger.info("list::"+list);
        logger.info("list.size()::"+list.size());
        joinConfig.getTcpIpConfig().setMembers(list);
//        joinConfig.getTcpIpConfig().setConnectionTimeoutSeconds(0);
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(true);
        AutoDetectionConfig autoDetectionConfig = new AutoDetectionConfig();
        autoDetectionConfig.setEnabled(true);
        joinConfig.setAutoDetectionConfig(autoDetectionConfig);
        
        MapConfig mapConfig = config.getMapConfig("DEMO_MAP");
        mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(new CustomPartitioningStrategy()));
        
        
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(3); // Number of CP members in the cluster
        cpSubsystemConfig.setGroupSize(3); // Number of members in each CP group
//        cpSubsystemConfig.setGroupName("my-cp-group"); // Name of the CP group
        
        config.getNetworkConfig().setPort(Integer.valueOf(environment.getProperty("port")));
        
        // Create a Hazelcast instance
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
     // Join the CP group
        CPSubsystem cpSubsystem = hazelcastInstance.getCPSubsystem();
        CPSubsystemManagementService managementService = cpSubsystem.getCPSubsystemManagementService();

     // Wait for membership
     try {
		managementService.awaitUntilDiscoveryCompleted(5, TimeUnit.MINUTES);
		logger.info("DiscoveryCompleted::"+managementService.isDiscoveryCompleted());
		CompletionStage<Collection<CPMember>> completionStage = managementService.getCPMembers();
		CompletableFuture future = completionStage.toCompletableFuture();
		future.get();
		IMap<String, String> map = hazelcastInstance.getMap("DEMO_MAP");
		EntryAddedListener<String, String> listener = new IMapListner(hazelcastInstance);
		map.addEntryListener(listener, true);
	} catch (InterruptedException | ExecutionException e) {
		e.printStackTrace();
	}
        return hazelcastInstance;
	} 
}
