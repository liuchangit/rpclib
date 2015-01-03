package com.liuchangit.rpclib.connpool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

public class ServiceHandle {
	
	private static final Logger LOG = Logger.getLogger("rpc");
	private ServiceGroupShard shard;
	private ServiceConfig config;
	private List<Connection> conns = new CopyOnWriteArrayList<Connection>();
	
	public ServiceHandle(ServiceGroupShard shard, ServiceConfig config) {
		super();
		this.shard = shard;
		this.config = config;
	}

	public void putCall(Call call){
		if (conns.size()>0){
			try{
				dispatch(call);
			}catch (Exception e) {
				finish(call);
			}
		}else{
			finish(call);
		}
	}
	
	private void dispatch(Call call){
		
		int size = conns.size();
		int idx = call.getSeqid()%size;
		int disFail = call.getDispatchFailCount();
		
		if (disFail>0){	//如果上次发送失败，更换互备服务分发请求
			if (config.getDispatchTimePreCall()>disFail){
				
				int retryIdx = idx;
				int activeReplica = size/config.getConnsPerAddress();
				if (activeReplica>1){//多个备份
					retryIdx = idx + (disFail*config.getConnsPerAddress());
				}else {
					retryIdx = size>1 ? (idx + disFail) : idx;
				}
				
				if (retryIdx>=size){
					retryIdx-=size;
				}
				if (retryIdx!=idx || size==1){
					idx = retryIdx;
				}else{
					finish(call);
					return;
				}
			}else {
				finish(call);
				return;
			}
		}
		
		
		
		if (conns.size()>idx && idx>-1){
			Connection guard = conns.get(idx);
			call.setServer(guard.getAddress());
			guard.sendCall(call);
			if(disFail>0){
				LOG.info("redispatch success\t" + call.getSeqid() + "\tfailCount:"+call.getDispatchFailCount() +"\tidx:" + idx +"\tconns:" + connSize() +"\tconns:" + this);
			}
		}else {
			finish(call);
		}
	}
	
	private void finish(Call call) {
		if (call.getDispatchFailCount()==0){
			call.noServer();
		}else if(call.getRejectCount()>0){
			call.serverReject();
		}else {
			call.serverError();
		}
		LOG.info("redispatch fail\t" + call.getSeqid() + "\tfailCount:"+call.getDispatchFailCount()  +"\ttryTimesPreCall:" +config.getDispatchTimePreCall() +"\tconns:" + connSize() +"\tconns:" + this);
	}

	void putConn(Connection conn){
		conns.add(conn);
	}
	
	void removeConn(Connection conn){
		conns.remove(conn);
	}
	
	int connSize(){
		return conns.size();
	}
	
	public String getShardLabel() {
		return this.shard.getLabel();
	}
	
	public Map<String, Object> getStats() {
		int pendingSize = 0;
		int awaitSize = 0;
		for (Connection conn : conns) {
			pendingSize += conn.getPendingSize();
			awaitSize += conn.getAwaitSize();
		}
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("pendingSize", pendingSize);
		map.put("awaitSize", awaitSize);
		return map;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ServiceHandle[");
		builder.append(shard.getClusterName()).append("] ConnSize:").append(conns.size());
		return builder.toString();
	}
}
