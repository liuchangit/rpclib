package com.liuchangit.rpclib.connpool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

/**
 * a connection object used by two roles: the business thread which tries to retrieve a connection and constructs one if none,
 * the selector thread which manages the rest of the life cycle of the connection object.
 * in concept, a connection belongs to a connection pool of a service address.
 * 
 * @author liuchang
 *
 */
public class Connection {
	private static final Logger LOG = Logger.getLogger("connection");
	private static final int MAX_RESULT_SIZE = 10000000;

	private final String name;
    
	private final SocketChannel channel;
	private final TProtocolFactory protFactory;
	private final ConnectionPool pool;
	
	boolean registered = false;
	private volatile SelectorThread selector;
	private volatile SelectionKey key;
	private int loops = 0;	//with pending calls, the times for which the selector loops while the connection in block state
							//once the connection becomes readable or writable, the counter is reset
	
	final Map<Integer, Call> awaitCalls = new HashMap<Integer, Call>();							//to be received after send 
	final ConcurrentLinkedQueue<Call> pendingCalls = new ConcurrentLinkedQueue<Call>();		//to be sent
	private Call writing = null;
    
	private final byte[] sizeBufferArray = new byte[4];
	private ByteBuffer readSizeBuffer = ByteBuffer.wrap(sizeBufferArray);
	private ByteBuffer readFrameBuffer;
    
	private final AtomicBoolean closing = new AtomicBoolean(false);
    
	Connection(ConnectionPool pool, SocketChannel channel, TProtocolFactory protFactory) {
    	this.name = "Connection(" + pool.address.toString() + "):" + channel.socket().getLocalPort();
    	this.pool = pool;
    	this.channel = channel;
    	this.protFactory = protFactory;
    	LOG.info(getName() + " starting");
    }
    
    String getName() {
    	return this.name;
    }
    
    void checkPending(SelectorThread selector) {
    	if (!registered) {
			try {
				SelectionKey key = channel.register(selector.getSelector(), SelectionKey.OP_READ);	// always read, to handle connection close
		    	key.attach(this);
		    	this.selector = selector;
		    	this.key = key;
		    	registered = true;
			} catch (Exception e) {
				close();
			}
    	}
    	checkTimeOutCall();
    	if (pendingCalls.size() > 0) {
			key.interestOps(getKey().interestOps() | SelectionKey.OP_WRITE);
			loops++;
		}
    }
    
    /**
     * 检查超时请求
     * 1 队列太长队尾请求等到超时
     * 2 多次分发到不可用的连接上直到超时
     * 3 请求已发出去，还未收回结果
     */
    private void checkTimeOutCall() {
		int sentCalls = 0;
		for (Integer key : awaitCalls.keySet()) {
			Call call = awaitCalls.get(key);
			if (!call.isTimedout() && call.isExpired()) {
				call.incremenUnreceived();
				call.receiveTimeout();
				sentCalls++;
			}
		}
		if (sentCalls > 0) {
			LOG.info(getName() + " has timedout calls(sent:" + sentCalls + ")");
		}
	}

	/* 
     * alive check. when a call arrives, checkPending will be executed at least once.
     * when the connection become readable or writable, readCall or writeCall will be executed.
     * if there is a circumstances under which a number of continued calls arrive,
     * but the connection has none read or write event occured during the time span,
     * we consider the connection is stale
	 */
    boolean checkAlive() {
    	int blockTimes = pool.config.getConnMaxBlockTimes();
    	if (loops > blockTimes && pendingCalls.size() > blockTimes) {
    		close();
    		return false;
    	}
    	return true;
    }
    
    SelectionKey getKey() {
    	return key;
    }
    
    public boolean sendCall(Call call) {
    	
    	if (closing.get()) {
    		returnCall(call);
    		return true;
    	}
    	
    	try {
	    	call.prepare(protFactory);
    	} catch (Exception e)  {
    		returnCall(call);
    		return true;
    	}
    	
    	pendingCalls.add(call);
    	if (selector != null) {
    		selector.wakeup();
    	}
    	return true;
    }
    
    void writeCall() throws Exception {
    	if (writing == null || !writing.getFrameBuffer().hasRemaining()) {
    		writing = nextPending();
    	}
    	if (writing != null) {
    		try {
	    		if (writing.getSizeBuffer().hasRemaining()) {	//middle in size buffer
	    			if (writing.getSizeBuffer().position() == 0) {
	    				writing.setStime(System.currentTimeMillis());
	    			}
	    			channel.write(writing.getSizeBuffer());
	    		}
	    		if (!writing.getSizeBuffer().hasRemaining() && writing.getFrameBuffer().hasRemaining()) {	//middle in frame buffer
	    			channel.write(writing.getFrameBuffer());
	    		}
	    		if (!writing.getFrameBuffer().hasRemaining()) {	//sending over, prepare receiving
	    			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
	    			
	    			writing = nextPending();
	    		}
    		} catch (Exception e) {
    			throw new Exception(e);
    		}
    	}
    	if (writing == null) {				//no pending calls, need not write
    		key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
    	}
    	loops = 0;
    	
    }

	private Call nextPending() {
		Call next = pendingCalls.poll();
		while (next != null && next.isExpired()) {
			next.sendTimeout();
			next = pendingCalls.poll();
		}
		if (next != null) {
			awaitCalls.put(next.getSeqid(), next);
		}
		return next;
	}
    
    void readCall() throws Exception {
    	if (readSizeBuffer.hasRemaining()) {		//middle in reading size
    		if (channel.read(readSizeBuffer) < 0) {
    			throw new IOException("EOF");
    		}
        	if (!readSizeBuffer.hasRemaining()) {	//just end reading size
        		int frameSize = TFramedTransport.decodeFrameSize(sizeBufferArray);
        		if (frameSize > MAX_RESULT_SIZE) {
        			throw new Exception("frame size is larger than max value(" + MAX_RESULT_SIZE + "): " + frameSize);
        		}
        		readFrameBuffer = ByteBuffer.allocate(frameSize);
        	}
    	}
    	if (!readSizeBuffer.hasRemaining() && readFrameBuffer.hasRemaining()) {	//middle in reading frame
    		if (channel.read(readFrameBuffer) < 0) {
    			throw new IOException("EOF");
    		}
        	if (!readFrameBuffer.hasRemaining()) {		//end reading frame
        		readSizeBuffer.rewind();				//prepare for next call's reading
        		TMemoryInputTransport memoryTransport = new TMemoryInputTransport(readFrameBuffer.array());
        		TProtocol prot = protFactory.getProtocol(memoryTransport);
        		TMessage msg = prot.readMessageBegin();
    			int seqid = msg.seqid;
    			Call call = awaitCalls.remove(seqid);
    			call.setRespSize(readFrameBuffer.capacity());
    			if (msg.type == TMessageType.EXCEPTION) {
    				TApplicationException x = TApplicationException.read(prot);
    				prot.readMessageEnd();
    				call.setError(x);
    				call.setTime((int)(System.currentTimeMillis() - call.getStime()));

    				if (!call.isTimedout()){
    					if (org.apache.thrift.TApplicationException.SERVER_REJECT == x.getType()){
        					call.incremenRejectCount();
        				}
    					returnCall(call);
    				}
    			} else {
    				call.getResult().read(prot);
    				prot.readMessageEnd();
    				call.setTime((int)(System.currentTimeMillis() - call.getStime()));
    				if (!call.isTimedout()){
    					call.success();
					}
    			}
    			call.setServer(pool.address);
    			
    			/*if (calls.isEmpty()) {			//no call to be received, so need not read
    				key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
    			}*/
        	}
    	}
    	loops = 0;
    }
    
    int getPendingSize() {
    	return this.pendingCalls.size();
    }
    
    int getAwaitSize() {
    	return this.awaitCalls.size();
    }
    
    void close() {
    	
    	if (closing.compareAndSet(false, true)) {
    		try {
		    	pool.invalidateConnection(this);
		    	this.channel.close();
		    	if (selector != null) {
			    	selector.unregister(this);
			    	key.attach(null);
			    	key.cancel();
		    	}
		    	Exception e = new Exception();
		    	StackTraceElement[] trace = e.getStackTrace();
		    	StackTraceElement caller = null;
		    	if (trace != null && trace.length > 1) {
		    		caller = trace[1];
		    	}
		    	LOG.info(getName() + " closing " + (caller == null ? " for unknown reason" : (" by " + caller)));
		    	
		    	cleanupCalls();
    		} catch (Exception e) {
    			
    		}
    	}
    }
    
    /**
     * 连接关闭，完成请求重新分发
     */
    private void cleanupCalls() {
    	try {
    		int pendingSize = pendingCalls.size();
    		Call call = null;
    		while ((call = pendingCalls.poll()) != null) {
    			returnCall(call);
    		}
    		int awaitSize = awaitCalls.size();
	    	for (Iterator<Map.Entry<Integer, Call>> iter = awaitCalls.entrySet().iterator(); iter.hasNext();) {
	    		Map.Entry<Integer, Call> entry = iter.next();
	    		Call unreceived = entry.getValue();
	    		unreceived.incremenUnreceived();
	    		returnCall(unreceived);
	    		iter.remove();
	    	}
	    	LOG.info(getName() + " close, cleanup calls and retry (pending:" + pendingSize + ", await:" + awaitSize + ")");
    	} catch (Exception e) {
	    	LOG.error(getName() + " close, cleanup calls exception", e);
    	}
    }
    
    public String toString() {
    	return this.name;
    }
    
    private void returnCall(Call call) {
    	call.incremenDispatchFailCount();
    	pool.handle.putCall(call);
    }
    
    public ServiceAddress getAddress() {
    	return pool.address;
    }
}

