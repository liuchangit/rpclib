package com.liuchangit.rpclib.connpool;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * A Call is an abstraction of RPC calls
 * @author liuchang
 *
 */
public class Call {
	private int seqid;
	private String func;
	private long stime;
	private volatile int time;
	private TBase args;
	private TBase result;
	private Exception error;
	
	private int reqSize;
	private int respSize;

	private static final int INITIAL_MEMORY_BUFFER_SIZE = 128;
	private ByteBuffer sizeBuffer;
	private ByteBuffer frameBuffer;
	
	private ReentrantLock lock = new ReentrantLock();
	private Condition completeCond = lock.newCondition();
	private volatile boolean complete = false;
	
	public final static int STATUS_PENDING = 0;
	public final static int STATUS_SUCCESS = 1;
	public final static int STATUS_SENDFAIL = 2;
	public final static int STATUS_SVRERROR = 3;
	public final static int STATUS_TIMEDOUT = 4;
	public final static int STATUS_SVREJECT = 5;
	public final static int STATUS_SENDTIMEOUT = 6;
	public final static int STATUS_NOSERVER = 7;
	
	private int status = STATUS_PENDING;
	
	private Callback callback = null;
	//请求发送失败计数，只要重新分发请求，则认为上次失败
	private int dispatchFailCount = 0;
	//请求超时时长，每个call都要设置次值,单位毫秒
	private int timeout = 0;
	//请求处理server
	private ServiceAddress server;
	//请求已发出未收到结果次数，可能超时了或conn不可用了。
	private int unreceivedCount = 0;
	//请求被拒绝次数
	private int rejectCount = 0;

	private long createTime = System.currentTimeMillis();

	public Call(int seqid, String func, TBase args, TBase result, int timeout){
		this.seqid = seqid;
		this.func = func;
		this.args = args;
		this.result = result;
		this.timeout = timeout;
	}
	
	public void setCallback(Callback callback) {
		this.callback = callback;
	}
    
    public int getSeqid() {
		return seqid;
	}

	public String getFunc() {
		return func;
	}

	public TBase getArgs() {
		return args;
	}

	public TBase getResult() {
		return result;
	}

	public Exception getError() {
		return error;
	}

	public void setError(Exception error) {
		this.error = error;
	}

	public int getReqSize() {
		return reqSize;
	}

	public int getRespSize() {
		return respSize;
	}
	
	void setReqSize(int reqSize) {
		this.reqSize = reqSize;
	}
	
	void setRespSize(int respSize) {
		this.respSize = respSize;
	}

	ByteBuffer getSizeBuffer() {
		return sizeBuffer;
	}

	ByteBuffer getFrameBuffer() {
		return frameBuffer;
	}
	
	void prepare(TProtocolFactory protFactory) throws Exception {
		TMemoryBuffer memoryBuffer = new TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE);
		TProtocol protocol = protFactory.getProtocol(memoryBuffer);
		protocol.writeMessageBegin(new org.apache.thrift.protocol.TMessage(getFunc(), org.apache.thrift.protocol.TMessageType.CALL, getSeqid()));
		getArgs().write(protocol);
		protocol.writeMessageEnd();

	    int length = memoryBuffer.length();
	    frameBuffer = ByteBuffer.wrap(memoryBuffer.getArray(), 0, length);
	    
	    byte[] sizeBufferArray = new byte[4];
	    TFramedTransport.encodeFrameSize(length, sizeBufferArray);
	    sizeBuffer = ByteBuffer.wrap(sizeBufferArray);
	    setReqSize(length);
	}

	public void waitComplete(int timeout) {
    	lock.lock();
    	try {
    		if (!complete) {
    			boolean b = completeCond.await(timeout, TimeUnit.MILLISECONDS);
    			if (!b) {
    				status = STATUS_TIMEDOUT;
    			}
    		}
    	} catch (InterruptedException e) {
    		
    	} finally {
    		lock.unlock();
    	}
    }
    
    public void success() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_SUCCESS;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onSuccess(this);
    	}
    }
    
    /**
     * 请求发送失败会重新分发到互备节点上
     */
    @Deprecated
    public void sendFail() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_SENDFAIL;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onError(this);
    	}
    }
    
    public void serverError() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_SVRERROR;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onError(this);
    	}
    }
    
    public void serverReject() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_SVREJECT;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onReject(this);
    	}
    }
    
    public void noServer() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_NOSERVER;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onError(this);
    	}
    }
    
    public void sendTimeout() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_SENDTIMEOUT;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onError(this);
    	}
    }
    
    public void receiveTimeout() {
    	lock.lock();
    	try {
    		complete = true;
    		status = STATUS_TIMEDOUT;
    		completeCond.signal();
    	} finally {
    		lock.unlock();
    	}
    	if (this.callback != null) {
    		this.callback.onError(this);
    	}
    }
    
    public boolean isComplete() {
    	return this.complete;
    }
    
    public int getStatus() {
    	return this.status;
    }
    
    public boolean isSuccess() {
    	return status == STATUS_SUCCESS;
    }
    
    /**
     * use isServerError instead
     * @return
     * @Deprecated
     */
    public boolean isFail() {
    	return isServerError();
    }
    
    public boolean isSentFail() {
    	return status == STATUS_SENDFAIL;
    }
    
    public boolean isServerError() {
    	return status == STATUS_SVRERROR;
    }
    
    public boolean isTimedout() {
    	return status == STATUS_TIMEDOUT || status == STATUS_SENDTIMEOUT;
    }
    
    public boolean isNoServer() {
    	return status == STATUS_NOSERVER ;
    }
    
	void setStime(long stime) {
		this.stime = stime;
	}

	long getStime() {
		return this.stime;
	}

	public int getTime() {
		return time;
	}

	public void setTime(int time) {
		this.time = time;
	}

	public int getDispatchFailCount() {
		return dispatchFailCount;
	}
  
	public void incremenDispatchFailCount(){
		dispatchFailCount++;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public ServiceAddress getServer() {
		return server;
	}

	public void setServer(ServiceAddress server) {
		this.server = server;
	}

	public int getUnreceivedCount() {
		return unreceivedCount;
	}
	
	public void incremenUnreceived(){
		unreceivedCount++;
	}
	
	public long getCreateTime() {
		return createTime;
	}

	public int getRejectCount() {
		return rejectCount;
	}

	public void incremenRejectCount(){
		rejectCount++;
	}

	boolean isExpired() {
		return getTimeout() > 0 && System.currentTimeMillis() >= getDeadline();
	}

	long getDeadline() {
		return getCreateTime() + getTimeout();
	}
}