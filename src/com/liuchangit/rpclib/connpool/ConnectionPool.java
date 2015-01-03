package com.liuchangit.rpclib.connpool;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

class ConnectionPool {
	private static final Logger LOG = Logger.getLogger("connection");
	private final ReentrantLock mainLock = new ReentrantLock();
	final ServiceAddress address;
	final ServiceConfig config;
	ServiceHandle handle;
	
	private List<Connection> connections = new ArrayList<Connection>();
	
	ConnectionPool(ServiceAddress address, ServiceConfig config, ServiceHandle handle) {
		if (address == null || config.getConnsPerAddress() <= 0) {
			throw new IllegalArgumentException();
		}
		this.address = address;
		this.config = config;
		this.handle = handle;
		initConnections(config.getConnsPerAddress());
	}
	
	@Deprecated
	public Connection getConnection(int seqid) {
		Connection connection = null;
		try {
			if (mainLock.tryLock(config.getWaitConnectionTime(), TimeUnit.MILLISECONDS)) {
				try {
					if (connections.size() > 0) {
						connection = doGetConnection(seqid);
					} else {
						initConnections(config.getConnsPerAddress());
						connection = doGetConnection(seqid);
					}
				} catch (Exception e) {
					LOG.error("ConnectionPool.getConnection no pending connection in the pool for:" + address, e);
				} finally {
					mainLock.unlock();
				}
			}
		} catch (Exception e) {
			LOG.warn("can not fetch connection to: " + address + " due to thread intterupted");
		}
		return connection;
	}
	
	private void initConnections(int size) {
		mainLock.lock();
		try {
			
			if (address.isOnline()){
				for (int i = 0; i < size; i++) {
					Connection connection = createConnection();
					if (connection != null) {
						connections.add(connection);
					} else {
						LOG.warn("ConnectionPool.initConnections fail, maybe server not running: " + address);
						break;
					}
				}
				LOG.warn("ConnectionPool:" + this.address + " init conns:" + connections.size());
			}
		} finally {
			mainLock.unlock();
		}
	}
	
	@Deprecated
	private Connection doGetConnection(int seqid) {
		Connection connection = null;
		if (connections.size() == 0) {
			return null;
		}
		int idx = Math.abs(seqid) % connections.size();
		connection = connections.get(idx);
		return connection;
	}
	
	Connection createConnection() {
		Connection connection = null;
		SocketChannel channel = null;
		try {
			//nonblocking connection initialization
			channel = SocketChannel.open();
			Socket socket = channel.socket();
		    socket.setSoLinger(false, 0);
		    socket.setTcpNoDelay(true);
		    
		    channel.configureBlocking(true);	//connect in block mode
		    InetSocketAddress addr = new InetSocketAddress(address.getHost(), address.getPort());
		    socket.connect(addr, config.getConnTimeout());
		    
		    channel.configureBlocking(false);
			connection = new Connection(this, channel, new GBKCompactProtocol.Factory());
			SelectorThread selector = getSelector();
			selector.register(connection);
			handle.putConn(connection);
		} catch (Exception e) {
			this.address.inactivate();	//maybe server not running or network disconnect
			LOG.warn("ConnectionPool error createing connection for address:" + address, e);
			if (connection != null) {
				connection.close();
				connection = null;
			} else {
				try {
					if (channel != null) {
						channel.close();
					}
				} catch (Exception ex) {}
			}
		}
		return connection;
	}
	
	void checkConnections(){
		int needSize = config.getConnsPerAddress()-connections.size();
		if(needSize>0){
			initConnections(needSize);
		}
	}
	
	SelectorThread getSelector() {
		SelectorThread[] selectors = config.getSelectors();
		assert selectors.length > 0;
		int minConns = selectors[0].getConnectionNum();
		int minIdx = 0;
		for (int i = 1; i < selectors.length; i++) {
			if (selectors[i].getConnectionNum() < minConns) {
				minConns = selectors[i].getConnectionNum();
				minIdx = i;
			}
		}
		return selectors[minIdx];
	}
	
	public void invalidateConnection(Connection connection) {
		if (connection == null || !connections.contains(connection)) {
			//impossible
			return;
		}
		mainLock.lock();
		try {
			connections.remove(connection);
			handle.removeConn(connection);
		} catch (Exception e) {
			//
		} finally {
			mainLock.unlock();
		}
		
		try {
			connection.close();	//the connection may be already closed, we do this for reliability
		} catch (Exception e) {
			
		}
	}
	
	public void close() {
		mainLock.lock();
		try {
			for (Iterator<Connection> iter = connections.iterator(); iter.hasNext(); ) {
				Connection conn = iter.next();
				handle.removeConn(conn);
				iter.remove();
				conn.close();
			}
		} finally {
			mainLock.unlock();
		}
	}
}