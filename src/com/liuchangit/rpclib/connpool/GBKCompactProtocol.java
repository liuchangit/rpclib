package com.liuchangit.rpclib.connpool;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

/**
 * A TProtocol implementation that serializing strings using GBK.
 * This implementation can reduce about 1/3 bandwidth when transfering chinese characters 
 * comparing to TCompactProtocol which using UTF-8.
 * 
 * @author liuchang
 *
 */
public class GBKCompactProtocol extends TProtocol {
	
	private static final String CHAR_ENCODING = "GB18030";

	public static class Factory implements TProtocolFactory {
		public Factory() {
		}

		public TProtocol getProtocol(TTransport trans) {
			return new GBKCompactProtocol(trans);
		}
	}

	private TCompactProtocol prot;
	
	public GBKCompactProtocol(TTransport trans) {
		super(null);
		prot = new TCompactProtocol(trans);
	}

	public String readString() throws TException {
		int length = readVarint32();

		if (length == 0) {
			return "";
		}

		try {
			if (prot.getTransport().getBytesRemainingInBuffer() >= length) {
				String str = new String(prot.getTransport().getBuffer(), prot.getTransport()
						.getBufferPosition(), length, CHAR_ENCODING);
				prot.getTransport().consumeBuffer(length);
				return str;
			} else {
				byte[] buf = null;
				if (length == 0) {
					buf = new byte[0];
				}

				buf = new byte[length];
				prot.getTransport().readAll(buf, 0, length);
				return new String(buf, CHAR_ENCODING);
			}
		} catch (UnsupportedEncodingException e) {
			throw new TException(CHAR_ENCODING + " not supported!");
		}
	}

	public void writeString(String str) throws TException {
		try {
			byte[] bytes = str.getBytes(CHAR_ENCODING);
			writeVarint32(bytes.length);
			prot.getTransport().write(bytes, 0, bytes.length);
		} catch (UnsupportedEncodingException e) {
			throw new TException(CHAR_ENCODING + " not supported! str:" + str);
		}
	}

	byte[] i32buf = new byte[5];
	void writeVarint32(int n) throws TException {
		int idx = 0;
		while (true) {
			if ((n & ~0x7F) == 0) {
				i32buf[idx++] = (byte) n;
				// writeByteDirect((byte)n);
				break;
				// return;
			} else {
				i32buf[idx++] = (byte) ((n & 0x7F) | 0x80);
				// writeByteDirect((byte)((n & 0x7F) | 0x80));
				n >>>= 7;
			}
		}
		prot.getTransport().write(i32buf, 0, idx);
	}

	int readVarint32() throws TException {
		int result = 0;
		int shift = 0;
		if (prot.getTransport().getBytesRemainingInBuffer() >= 5) {
			byte[] buf = prot.getTransport().getBuffer();
			int pos = prot.getTransport().getBufferPosition();
			int off = 0;
			while (true) {
				byte b = buf[pos + off];
				result |= (int) (b & 0x7f) << shift;
				if ((b & 0x80) != 0x80)
					break;
				shift += 7;
				off++;
			}
			prot.getTransport().consumeBuffer(off + 1);
		} else {
			while (true) {
				byte b = readByte();
				result |= (int) (b & 0x7f) << shift;
				if ((b & 0x80) != 0x80)
					break;
				shift += 7;
			}
		}
		return result;
	}

	public TStruct readStructBegin() throws TException {
		return prot.readStructBegin();
	}

	public TTransport getTransport() {
		return prot.getTransport();
	}

	public ByteBuffer readBinary() throws TException {
		return prot.readBinary();
	}

	public boolean readBool() throws TException {
		return prot.readBool();
	}

	public byte readByte() throws TException {
		return prot.readByte();
	}

	public double readDouble() throws TException {
		return prot.readDouble();
	}

	public TField readFieldBegin() throws TException {
		return prot.readFieldBegin();
	}

	public void readFieldEnd() throws TException {
		prot.readFieldEnd();
	}

	public short readI16() throws TException {
		return prot.readI16();
	}

	public int readI32() throws TException {
		return prot.readI32();
	}

	public long readI64() throws TException {
		return prot.readI64();
	}

	public TList readListBegin() throws TException {
		return prot.readListBegin();
	}

	public void readListEnd() throws TException {
		prot.readListEnd();
	}

	public TMap readMapBegin() throws TException {
		return prot.readMapBegin();
	}

	public void readMapEnd() throws TException {
		prot.readMapEnd();
	}

	public TMessage readMessageBegin() throws TException {
		return prot.readMessageBegin();
	}

	public void readMessageEnd() throws TException {
		prot.readMessageEnd();
	}

	public TSet readSetBegin() throws TException {
		return prot.readSetBegin();
	}

	public void readSetEnd() throws TException {
		prot.readSetEnd();
	}

	public void readStructEnd() throws TException {
		prot.readStructEnd();
	}

	public void reset() {
		prot.reset();
	}

	public String toString() {
		return prot.toString();
	}

	public void writeBinary(ByteBuffer bin) throws TException {
		prot.writeBinary(bin);
	}

	public void writeBool(boolean b) throws TException {
		prot.writeBool(b);
	}

	public void writeByte(byte b) throws TException {
		prot.writeByte(b);
	}

	public void writeDouble(double dub) throws TException {
		prot.writeDouble(dub);
	}

	public void writeFieldBegin(TField field) throws TException {
		prot.writeFieldBegin(field);
	}

	public void writeFieldEnd() throws TException {
		prot.writeFieldEnd();
	}

	public void writeFieldStop() throws TException {
		prot.writeFieldStop();
	}

	public void writeI16(short i16) throws TException {
		prot.writeI16(i16);
	}

	public void writeI32(int i32) throws TException {
		prot.writeI32(i32);
	}

	public void writeI64(long i64) throws TException {
		prot.writeI64(i64);
	}

	public void writeListBegin(TList list) throws TException {
		prot.writeListBegin(list);
	}

	public void writeListEnd() throws TException {
		prot.writeListEnd();
	}

	public void writeMapBegin(TMap map) throws TException {
		prot.writeMapBegin(map);
	}

	public void writeMapEnd() throws TException {
		prot.writeMapEnd();
	}

	public void writeMessageBegin(TMessage message) throws TException {
		prot.writeMessageBegin(message);
	}

	public void writeMessageEnd() throws TException {
		prot.writeMessageEnd();
	}

	public void writeSetBegin(TSet set) throws TException {
		prot.writeSetBegin(set);
	}

	public void writeSetEnd() throws TException {
		prot.writeSetEnd();
	}

	public void writeStructBegin(TStruct struct) throws TException {
		prot.writeStructBegin(struct);
	}

	public void writeStructEnd() throws TException {
		prot.writeStructEnd();
	}
}
