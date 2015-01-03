package com.liuchangit.rpclib.server;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

import com.liuchangit.rpclib.server.TAsyncServer.MessageBuffer;

public class Aggregator<I> implements Runnable {
	final MessageQueue<Message> mq;
	final TProtocolFactory protoFactory;
	final Map<String, AsyncProcessFunction> functionMap;
	final I processor;
	long waitNanos = 10L;
	volatile boolean stop = false;
	
	public Aggregator(MessageQueue<Message> mq, TProtocolFactory protoFactory, Map<String, AsyncProcessFunction> functionMap, I processor) {
		this.mq = mq;
		this.protoFactory = protoFactory;
		this.functionMap = functionMap;
		this.processor = processor;
	}
	
	public void stop() {
		stop = true;
	}
	
	public long getWaitNanos() {
		return waitNanos;
	}

	public void setWaitNanos(long waitNanos) {
		this.waitNanos = waitNanos;
	}
	
	public void run() {
		while (!stop) {
			Message msg = mq.poll();
			if (msg != null) {
				try {
					processMessage(msg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				LockSupport.parkNanos(waitNanos);
			}
		}
	}
	

	private void processMessage(Message msg) throws Exception {
		MessageBuffer mb = msg.getMessageBuffer();
		String methodName = mb.getMethod();
		AsyncProcessFunction function = functionMap.get(methodName);
		try {
			TBase result = function.postprocess(msg, processor);
			TByteArrayOutputStream byteArray = new TByteArrayOutputStream();
			TProtocol out = protoFactory.getProtocol(new TIOStreamTransport(byteArray));
			out.writeMessageBegin(new TMessage(methodName, TMessageType.REPLY, mb.getSeqid()));
			result.write(out);
			out.writeMessageEnd();
			mb.setResponseBuffer(byteArray.get(), 0, byteArray.len());
		} catch (Exception e) {
			TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
			TByteArrayOutputStream byteArray = new TByteArrayOutputStream();
			TProtocol out = protoFactory.getProtocol(new TIOStreamTransport(byteArray));
			out.writeMessageBegin(new TMessage(methodName, TMessageType.EXCEPTION, mb.getSeqid()));
			x.write(out);
			out.writeMessageEnd();
			mb.setResponseBuffer(byteArray.get(), 0, byteArray.len());
		}
	}
}
