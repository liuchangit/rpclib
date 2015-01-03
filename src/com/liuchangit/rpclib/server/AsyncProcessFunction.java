package com.liuchangit.rpclib.server;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;

import com.liuchangit.rpclib.server.TAsyncServer.MessageBuffer;

public abstract class AsyncProcessFunction<I, T extends TBase> {
	
	protected final String methodName;
	
	public AsyncProcessFunction(String methodName) {
		this.methodName = methodName;
	}
	
	public abstract void preprocess(MessageBuffer msg, TProtocol iprot, I processor) throws Exception;
	public abstract T postprocess(Message msg, I processor) throws Exception;

}
