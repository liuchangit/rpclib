package com.liuchangit.rpclib.connpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceGroup {
	ConcurrentHashMap<String, ServiceGroupShard> shardMap = new ConcurrentHashMap<String, ServiceGroupShard>();
	
	public ServiceGroup() {
		
	}
	
	public ServiceGroupShard putShard(String shardLabel, ServiceGroupShard shard) {
		ServiceGroupShard old = shardMap.putIfAbsent(shardLabel, shard);
		if (old != null) {
			return old;
		}
		return shard;
	}
	
	public ServiceGroupShard getShard(String shardLabel) {
		return shardMap.get(shardLabel);
	}
	
	public String[] getShardLabels() {
		return shardMap.keySet().toArray(new String[0]);
	}

	public List<ServiceGroupShard> getShards() {
		List<ServiceGroupShard> shards = new ArrayList<ServiceGroupShard>();
		shards.addAll(shardMap.values());
		return shards;
	}

	public int size() {
		return shardMap.size();
	}
	
	public String toString() {
		return shardMap.toString();
	}
}
