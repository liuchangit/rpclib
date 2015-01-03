package com.liuchangit.rpclib.connpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceGroupShard {
	final String label;
	String clusterName;
	List<ServiceAddress> replicas = new CopyOnWriteArrayList<ServiceAddress>();
	AtomicInteger selectCount = new AtomicInteger(0);
	ServiceHandle handle;
	
	public ServiceGroupShard(String label) {
		this.label = label;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	public void addReplica(ServiceAddress replica) {
		if (!replicas.contains(replica)) {
			replicas.add(replica);
		}
	}
	
	public void removeReplica(ServiceAddress replica) {
		replicas.remove(replica);
	}

	public List<ServiceAddress> getReplicas() {
		List<ServiceAddress> addrs = new ArrayList<ServiceAddress>();
		addrs.addAll(replicas);
		return addrs;
	}
	
	public boolean contains(ServiceAddress replica) {
		return replicas.contains(replica);
	}
	
	public ServiceAddress selectAddress() {
		ServiceAddress addr = null;
		int replicaCnt = replicas.size();
		if (replicaCnt > 0) {
			int tryCnt = 0;
			do {
				int selectCnt = selectCount.getAndIncrement();
				addr = replicas.get(selectCnt % replicaCnt);
				if (!addr.isOnline() || !addr.isActive()) {
					addr = null;
				}
			} while ((tryCnt++) < replicaCnt && addr == null);
		}
		return addr;
	}
	
	public int size() {
		return replicas.size();
	}
	
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof ServiceGroupShard) {
			ServiceGroupShard other = (ServiceGroupShard)o;
			return this.replicas.equals(other.replicas);
		}
		return false;
	}
	
	public int hashCode() {
		int result = 17;
		result = 31 * result + this.replicas.hashCode();
		return result;
	}
	
	public String toString() {
		return replicas.toString();
	}

	public ServiceHandle getHandle() {
		return handle;
	}

	public void setHandle(ServiceHandle handle) {
		this.handle = handle;
	}
}
