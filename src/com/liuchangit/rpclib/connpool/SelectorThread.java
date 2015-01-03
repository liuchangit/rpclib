package com.liuchangit.rpclib.connpool;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

class SelectorThread extends Thread {
	private static final Logger LOG = Logger.getLogger("connection");
	
	private final Selector selector;
	private final ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<Connection>();		//connected
	
	public SelectorThread(ThreadGroup group) throws IOException {
		super(group, group.getName() + " Selector");
		this.selector = SelectorProvider.provider().openSelector();
		this.setDaemon(true);
	}
	
	public void run() {
		try {
			while (true) {
				try {
					try {
						selector.select();
					} catch (Exception e) {
						LOG.error(getName() + " select() error", e);
					}
					doIoTask();
					checkConnections();
				} catch (Exception e) {
					LOG.error(getName() + " ignore error", e);
				}
			}
		} catch (Throwable t) {
			LOG.fatal(getName() + " fatal error", t);
		}
	}
	
	private void checkConnections() {
		for (Connection conn : connections) {
			if (conn.checkAlive()) {
				conn.checkPending(this);
			}
		}
	}

	private void doIoTask() {
		try {
			Set<SelectionKey> keys = selector.selectedKeys();
			Iterator<SelectionKey> it = keys.iterator();
			while (it.hasNext()) {
				SelectionKey key = it.next();
				Connection conn = (Connection)key.attachment();
				it.remove();
				if (!key.isValid()) {
					conn.close();
					key.cancel();
					key.attach(null);
					continue;
				}
				if (key.isReadable()) {
					try {
						conn.readCall();
					} catch (Exception e) {
						LOG.warn(getName() + " read " + conn + " failed", e);
						conn.close();
						continue;
					}
				}
				if (key.isWritable()) {
					try {
						conn.writeCall();
					} catch (Exception e) {
						LOG.warn(getName() + " write " + conn + " failed", e);
						conn.close();
						continue;
					}
				}
			}
		} catch (Exception e) {
			LOG.error(getName() + " error doing io", e);
		}
	}
	
	public void wakeup() {
		selector.wakeup();
	}
	
	public Selector getSelector() {
		return this.selector;
	}
	
	public void register(Connection conn) {
		connections.add(conn);
		selector.wakeup();
	}
	
	public void unregister(Connection conn) {
		connections.remove(conn);
	}
	
	public int getConnectionNum() {
		return connections.size();
	}
}
