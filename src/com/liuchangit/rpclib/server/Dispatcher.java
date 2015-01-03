package com.liuchangit.rpclib.server;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

import com.liuchangit.rpclib.server.TAsyncServer.MessageBuffer;

public class Dispatcher<I> implements Runnable {
	final MessageQueue<MessageBuffer> mq;
	final TProtocolFactory protoFactory;
	final Map<String, AsyncProcessFunction> functionMap;
	final I processor;
	long waitNanos = 10L;
	volatile boolean stop = false;
	
	public Dispatcher(MessageQueue<MessageBuffer> mq, TProtocolFactory protoFactory, Map<String, AsyncProcessFunction> functionMap, I processor) {
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
			MessageBuffer mb = mq.poll();
			if (mb != null) {
				try {
					processMessage(mb);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				LockSupport.parkNanos(waitNanos);
			}
		}
	}
	
	private void processMessage(MessageBuffer mb) throws Exception {
		byte[] buf = mb.getRequestBuffer();
		TMemoryInputTransport trans = new TMemoryInputTransport(buf);
		TProtocol proto = protoFactory.getProtocol(trans);
		TMessage msg = proto.readMessageBegin();
		mb.setMethod(msg.name);
		mb.setSeqid(msg.seqid);
		AsyncProcessFunction function = functionMap.get(msg.name);
		try {
			if (function == null) {
				TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
				TByteArrayOutputStream byteArray = new TByteArrayOutputStream();
				TProtocol out = protoFactory.getProtocol(new TIOStreamTransport(byteArray));
				out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
				x.write(out);
				out.writeMessageEnd();
				mb.setResponseBuffer(byteArray.get(), 0, byteArray.len());
				return;
			}
			function.preprocess(mb, proto, processor);
		} catch (Exception e) {
			TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
			TByteArrayOutputStream byteArray = new TByteArrayOutputStream();
			TProtocol out = protoFactory.getProtocol(new TIOStreamTransport(byteArray));
			out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
			x.write(out);
			out.writeMessageEnd();
			mb.setResponseBuffer(byteArray.get(), 0, byteArray.len());
		}
	}
}
