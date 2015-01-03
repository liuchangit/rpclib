package com.liuchangit.rpclib.connpool;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ServicePool {
	
	private ServiceConfig config;
	
	private Thread maintThread;
	
	final ConcurrentHashMap<ServiceAddress, ServiceAddress> addresses = new ConcurrentHashMap<ServiceAddress, ServiceAddress>();
	
	/* name -> addresses */
	final ConcurrentHashMap<String, ServiceGroup> groups = new ConcurrentHashMap<String, ServiceGroup>();
	
	public ServicePool() {
		this(true);
	}
	
	public ServicePool(String name) {
		this(name, true);
	}
	
	public ServicePool(boolean detect) {
		this("Server", 1, 500, detect);
	}
	
	public ServicePool(String name, boolean detect) {
		this(name, 1, 500, detect);
	}
	
	public ServicePool(String name, int socketsPerAddress, int connTimeout, boolean detect) {
		this(name, socketsPerAddress, connTimeout, detect, 1);
	}
	
	public ServicePool(String name, int connsPerAddress, int connTimeout, boolean detect, int selectorCount) {
		ServiceConfig config = new ServiceConfig();
		config.setServiceName(name);
		config.setConnsPerAddress(connsPerAddress);
		config.setConnTimeout(connTimeout);
		config.setDetect(detect);
		config.setSelectorCount(selectorCount);
		init(config);
	}
	
	public ServicePool(ServiceConfig config) {
		init(config);
	}

	private void init(ServiceConfig config) {
		this.config = config;
		try {
			config.initSelectors();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (config.isDetect()) {
			startMaintThread(config);
		}
	}
	
	void startMaintThread(ServiceConfig config) {
		maintThread = new DetectThread(this, config);
		maintThread.start();
	}
	
	public ServiceGroup getGroup() {
		return groups.get(ServiceConfig.DEFAULT_GROUP_NAME);
	}
	
	public ServiceGroup getGroup(String name) {
		return groups.get(name);
	}
	
	public String[] getGroupNames() {
		return groups.keySet().toArray(new String[0]);
	}
	
	/**
	 * 添加备份地址，无切片
	 * @param address
	 */
	public void putAddress(ServiceAddress address) {
		putAddress(ServiceConfig.DEFAULT_GROUP_NAME, ServiceConfig.DEFAULT_SHARD_LABEL, address);
	}
	
	/**
	 * 添加某类服务地址，无备份
	 * @param groupName
	 * @param address
	 */
	public void putAddress(String groupName, ServiceAddress address) {
		putAddress(groupName, ServiceConfig.DEFAULT_SHARD_LABEL, address);
	}
	
	/**
	 * 添加切片服务地址，不区分类别
	 * @param groupName
	 * @param address
	 */
	public void putShardAddress(String shardLabel, ServiceAddress address) {
		putAddress(ServiceConfig.DEFAULT_GROUP_NAME, shardLabel, address);
	}
	
	/**
	 * 添加某类服务某个切片的地址
	 * @param groupName
	 * @param shardLabel
	 * @param address
	 */
	public void putAddress(String groupName, String shardLabel, ServiceAddress address) {
		ServiceGroup group = groups.get(groupName);
		if (group == null) {
			group = new ServiceGroup();
			ServiceGroup old = groups.putIfAbsent(groupName, group);
			if (old != null) {
				group = old;
			}
		}
		
		String clusterName = config.getServiceName();
		if (!"".equals(groupName) && !ServiceConfig.DEFAULT_GROUP_NAME.equals(groupName)) {
			clusterName += "/" + groupName;
		}
		if (!"".equals(shardLabel) && !ServiceConfig.DEFAULT_SHARD_LABEL.equals(shardLabel)) {
			clusterName += "/" + shardLabel;
		}
		
		ServiceGroupShard shard = group.getShard(shardLabel);
		if (shard == null) {
			shard = new ServiceGroupShard(shardLabel);
			shard.setClusterName(clusterName);
			shard = group.putShard(shardLabel, shard);
			ServiceHandle handle =  new ServiceHandle(shard, config);
			shard.setHandle(handle);
		}
		ServiceHandle handle = shard.getHandle();
		address.setClusterName(clusterName);
		shard.addReplica(address);
		address.initConnections(config, handle);
		addresses.put(address, address);
	}
	
	public void removeAddress(ServiceAddress address) {
		removeAddress(ServiceConfig.DEFAULT_GROUP_NAME, ServiceConfig.DEFAULT_SHARD_LABEL, address);
	}
	
	public void removeAddress(String groupName, ServiceAddress address) {
		removeAddress(groupName, ServiceConfig.DEFAULT_SHARD_LABEL, address);
	}
	
	public void removeShardAddress(String shardLabel, ServiceAddress address) {
		removeAddress(ServiceConfig.DEFAULT_GROUP_NAME, shardLabel, address);
	}
	
	public void removeAddress(String groupName, String shardLabel, ServiceAddress address) {
		address = addresses.remove(address);
		ServiceGroup group = groups.get(groupName);
		if (group != null && address != null) {
			ServiceGroupShard shard = group.getShard(shardLabel);
			if (shard != null) {
				shard.removeReplica(address);
				address.closeConnections();
			}
		}
//		addresses.remove(address);
	}
	
	@Deprecated
	public List<ServiceAddress> selectAddresses() {
		return selectAddresses(ServiceConfig.DEFAULT_GROUP_NAME);
	}
	
	public List<ServiceHandle> getServiceHandles(){
		return getServiceHandles(ServiceConfig.DEFAULT_GROUP_NAME);
	}
	
	public List<ServiceHandle> getServiceHandles(String groupName){
		List<ServiceHandle> handles = new ArrayList<ServiceHandle>();
		ServiceGroup group = groups.get(groupName);
		if (group == null) {
			return handles;
		}
		for (String shardLabel : group.getShardLabels()) {
			ServiceGroupShard shard = group.getShard(shardLabel);
			handles.add(shard.getHandle());
		}
		return handles;
	}
	
	public ServiceHandle getServiceHandle(String groupName) {
		List<ServiceHandle> handles = getServiceHandles(groupName);
		if (handles.size()>0){
			return handles.get(0);
		}else {
			return null;
		}
	}
	
	public ServiceHandle getServiceHandle(String groupName, String shard) {
		ServiceGroup group = groups.get(groupName);
		if (group!=null){
			return group.getShard(shard).getHandle();
		}else{
			return null;
		}
	}
	
	@Deprecated
	public List<ServiceAddress> selectAddresses(String groupName) {
		ServiceGroup group = groups.get(groupName);
		if (group == null) {
			return null;
		}
		List<ServiceAddress> addresses = new ArrayList<ServiceAddress>();
		for (String shardLabel : group.getShardLabels()) {
			ServiceGroupShard shard = group.getShard(shardLabel);
			ServiceAddress address = shard.selectAddress();
			if (address != null) {
				addresses.add(address);
			}
		}
		return addresses;
	}
	
	@Deprecated
	public ServiceAddress selectAddress(String groupName) {
		List<ServiceAddress> addresses = selectAddresses(groupName);
		if (addresses != null && addresses.size() > 0) {
			return addresses.get(0);
		}
		return null;
	}
	
	public String toString() {
		return groups.toString();
	}
	
	/**
	 * 强制上下线
	 * @param addr
	 * @param online
	 * @return
	 */
	public boolean setOnline(String host, int port) {
		ServiceAddress key = new ServiceAddress(host, port);
		ServiceAddress addr = addresses.get(key);
		if (addr != null) {
			addr.setOnline();
			return true;
		}
		return false;
	}
	
	public boolean setOffline(String host, int port) {
		ServiceAddress key = new ServiceAddress(host, port);
		ServiceAddress addr = addresses.get(key);
		if (addr != null) {
			addr.setOffline();
			return true;
		}
		return false;
	}
	
	public boolean isAddrAvailable(String host, int port) {
		ServiceAddress key = new ServiceAddress(host, port);
		ServiceAddress addr = addresses.get(key);
		return addr.isOnline() && addr.isActive();
	}
	
	public boolean contains(ServiceAddress addr) {
		return addresses.contains(addr);
	}
}

class DetectThread extends Thread {
	private ServicePool pool;
	private ServiceConfig config;
	
	public DetectThread(ServicePool pool, ServiceConfig config) {
		this.pool = pool;
		this.config = config;
		this.setDaemon(true);
		this.setName("ServicePool>DetectThread");
	}
	
	public void run() {
		while (true) {
			try {
				sleep(config.getDetectInterval());
				Iterator<ServiceAddress> iter = pool.addresses.values().iterator();
				int retries = config.getDetectRetries();
				while (iter.hasNext()) {
					ServiceAddress addr = iter.next();
					boolean succ = tryConnect(addr, config.getConnTimeout());	//we try to connect the server to detect if it alive
					if (!succ) {
						for (int i = 0; i < retries; i++) {			//retry only if we meet a connecting fail
							succ = tryConnect(addr, config.getDetectConnectTime());
							if (succ) {
								break;
							}
						}
					}
					if (succ) {
						addr.activate();
					} else {
						addr.inactivate();
					}
				}
			} catch (Throwable e) {
				continue;
			}
		}
	}
	
	boolean tryConnect(ServiceAddress addr, int connTimeout) {
		boolean success = false;
		Socket socket = null;
		try {
			socket = new Socket();
			socket.setSoLinger(true, 0);
			socket.connect(new InetSocketAddress(addr.getHost(), addr.getPort()), connTimeout);
			success = true;
		} catch (Exception e) {
			;
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (Exception e) {}
			}
		}
		return success;
	}
	
}