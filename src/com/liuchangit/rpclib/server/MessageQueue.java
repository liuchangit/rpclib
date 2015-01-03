package com.liuchangit.rpclib.server;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueue<T> {
	private ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();
	private final AtomicInteger size = new AtomicInteger(0);
	public boolean offer(T e) {
		if (queue.add(e)) {
			size.incrementAndGet();
			return true;
		}
		return false;
	}
	
	public T poll() {
		T mb = queue.poll();
		if (mb != null) {
			size.decrementAndGet();
		}
		return mb;
	}
	
	public int size() {
		return size.get();
	}
	
	public void clear() {
		queue.clear();
		size.set(0);
	}
}
