package com.liuchangit.rpclib.connpool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * a ServiceAddress object represents a socket server's address, it contains a host and a port, 
 * a connection pool which holds all socket connections from localhost to the server,
 * it also has 2 types of states: active and inactive triggered by the surrounding application's runtime automatically, 
 * online and offline triggered by OP manually through the application's interface.
 * @author liuchang
 *
 */
public class ServiceAddress {

	private static final Logger LOG = Logger.getLogger("connection");
	final String host;
	final int port;
	volatile ConnectionPool pool = null;
	volatile String clusterName = "";
	
	static final String HOST_IP = getLocalEth0HostIp();
	
	private AtomicBoolean active = new AtomicBoolean(true);
	private AtomicLong inactiveTime = new AtomicLong(0);

	private volatile boolean online = true;
	
	
	public ServiceAddress(String host, int port) {
		this(new InetSocketAddress(host, port));
	}
	
	public ServiceAddress(InetSocketAddress socketAddress) {
		this.host = socketAddress.getAddress().getHostAddress();
		this.port = socketAddress.getPort();
	}
	
	public ServiceAddress(String hostport) {
		String[] parts = hostport.split(":");
		this.host = parts[0];
		this.port = Integer.parseInt(parts[1]);
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	String getClusterName() {
		return clusterName;
	}
	
	void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	void initConnections(ServiceConfig config, ServiceHandle handle) {
		this.pool = new ConnectionPool(this, config, handle);
	}
	
	void closeConnections() {
		if (pool != null) {
			this.pool.close();
		}
	}
	
	@Deprecated
	public Connection getConnection(int hash) {
		Connection conn = pool.getConnection(hash);
		if (conn == null ) {	//server not running
			//we already detect this exception when tries to create new connection, should log again?
		}
		
		return conn;
	}
	
	public void fail(Connection conn) {
		//do nothing, just keep api compatible
	}
	
	public void timeout(Connection conn) {
		//do nothing, just keep api compatible
	}
	
	public void inactivate() {
		if (active.compareAndSet(true, false)) {
			LOG.info("detect addr:" + toString() + " active -> inactive");
			long now = System.currentTimeMillis();
			inactiveTime.set(now);
			closeConnections();
		}
	}
	
	public void activate() {
		if (active.compareAndSet(false, true)) {
			LOG.info("detect addr:" + toString() + " inactive -> active");
			inactiveTime.set(0);
		}
		if (isOnline() && active.get()){
			checkValidConns();
		}
	}
	
	private void checkValidConns() {
		if (pool!=null){
			pool.checkConnections();
		}
	}

	public boolean isActive() {
		return active.get();
	}
	
	public long getInactiveTime() {
		return inactiveTime.get();
	}
	
	/**
	 * 上线并激活
	 */
	void setOnline() {
		this.online = true;
		onlineReport();
	}
	
	void setOffline() {
		this.online = false;
		closeConnections();
		offlineReport();
	}
	
	public boolean isOnline() {
		return this.online;
	}
	
	void offlineReport() {
		String msg = getReportString() + " offline";
//		MailSender.send(msg);
		LOG.info(msg);
	}
	
	void onlineReport() {
		String msg = getReportString() + " online";
//		MailSender.send(msg);
		LOG.info(msg);
	}
	
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof ServiceAddress) {
			ServiceAddress other = (ServiceAddress) o;
			return this.host.equals(other.host) && this.port == other.port;
		}
		return false;
	}
	
	public int hashCode() {
		int result = 17;
		result = 31 * result + host.hashCode();
		result = 31 * result + port;
		return result;
	}
	
	public String toString() {
		return host + ":" + port;
	}
	
	String getReportString() {
		return clusterName + "/" + toString();
	}

	static String getLocalEth0HostIp(){
		try {
			NetworkInterface ni = NetworkInterface.getByName("eth0");
			if (ni == null) {
				ni = NetworkInterface.getByName("eth1");
			}
			if (ni != null) {
				Enumeration<InetAddress> ips = ni.getInetAddresses();
				while (ips.hasMoreElements()) {
					String ip = ips.nextElement().getHostAddress();
					if (ip != null && ip.matches("(\\d{1,3}\\.){3}\\d{1,3}")) {
						return ip;
					}
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	static String getHostIp() {
		return HOST_IP;
	}
}

