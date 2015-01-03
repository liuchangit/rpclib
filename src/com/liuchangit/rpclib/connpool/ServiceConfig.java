package com.liuchangit.rpclib.connpool;


public class ServiceConfig {
	public static final String DEFAULT_SHARD_LABEL = "default_shard";
	public static final String DEFAULT_GROUP_NAME = "default_group";
	
	private String serviceName = "Server";

	private int connsPerAddress = 1;
	private int connTimeout = 500;
	private int connMaxBlockTimes = 100;
	private int waitConnectionTime = 1000;	//default set to 2*connTimeout
	private int soTimeout = 1000;
	
	private boolean detect = true;
	private int detectInterval = 1000;
	private int detectConnectTime = 200;
	private int detectRetries = 2;

	private int selectorCount = 1;
	private int dispatchTimePreCall = 2;
	
	private SelectorThread[] selectors;
	
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	
	public int getConnsPerAddress() {
		return connsPerAddress;
	}

	public void setConnsPerAddress(int connsPerAddress) {
		this.connsPerAddress = connsPerAddress;
	}

	public int getConnTimeout() {
		return connTimeout;
	}

	public void setConnTimeout(int connTimeout) {
		this.connTimeout = connTimeout;
	}

	public int getConnMaxBlockTimes() {
		return connMaxBlockTimes;
	}

	public void setConnMaxBlockTimes(int connMaxBlockTimes) {
		this.connMaxBlockTimes = connMaxBlockTimes;
	}

	public int getWaitConnectionTime() {
		return waitConnectionTime;
	}

	public void setWaitConnectionTime(int waitConnectionTime) {
		this.waitConnectionTime = waitConnectionTime;
	}

	public int getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public int getSelectorCount() {
		return selectorCount;
	}

	public void setSelectorCount(int selectorCount) {
		this.selectorCount = selectorCount;
	}

	public boolean isDetect() {
		return detect;
	}

	public void setDetect(boolean detect) {
		this.detect = detect;
	}

	public int getDetectInterval() {
		return detectInterval;
	}

	public void setDetectInterval(int detectInterval) {
		this.detectInterval = detectInterval;
	}

	public int getDetectConnectTime() {
		return detectConnectTime;
	}

	public void setDetectConnectTime(int detectConnectTime) {
		this.detectConnectTime = detectConnectTime;
	}

	public int getDetectRetries() {
		return detectRetries;
	}

	public void setDetectRetries(int detectRetries) {
		this.detectRetries = detectRetries;
	}

	public void initSelectors() throws Exception {
		ThreadGroup group = new ThreadGroup(getServiceName());
		this.selectors = new SelectorThread[selectorCount];
		for (int i = 0; i < selectorCount; i++) {
			selectors[i] = new SelectorThread(group);
			selectors[i].start();
		}
	}
	
	public SelectorThread[] getSelectors() {
		return selectors;
	}

	public int getDispatchTimePreCall() {
		return dispatchTimePreCall;
	}

	public void setDispatchTimePreCall(int dispatchTimePreCall) {
		this.dispatchTimePreCall = dispatchTimePreCall;
	}
	
}
