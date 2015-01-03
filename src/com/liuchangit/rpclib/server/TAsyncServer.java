package com.liuchangit.rpclib.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

public class TAsyncServer extends TServer {
	private static final Logger LOG = Logger.getLogger("service");

	public static class Args extends AbstractAsyncServerArgs<Args> {

		private MessageQueue<MessageBuffer> pendingQueue;
		
		public Args(TNonblockingServerTransport transport) {
			super(transport);
		}
		
		public Args pendingQueue(MessageQueue<MessageBuffer> pendingQueue) {
			this.pendingQueue = pendingQueue;
			return this;
		}
	}

	public static abstract class AbstractAsyncServerArgs<T extends AbstractAsyncServerArgs<T>>
			extends AbstractServerArgs<T> {
		public long maxReadBufferBytes = Long.MAX_VALUE;

		public AbstractAsyncServerArgs(
				TNonblockingServerTransport transport) {
			super(transport);
			transportFactory(new TFramedTransport.Factory());
		}
	}

	private final MessageQueue<MessageBuffer> pendingQueue;

	// Flag for stopping the server
	private volatile boolean stopped_ = true;

	private SelectThread selectThread_;

	/**
	 * The maximum amount of memory we will allocate to client IO buffers at a
	 * time. Without this limit, the server will gladly allocate client buffers
	 * right into an out of memory exception, rather than waiting.
	 */
	private final long MAX_READ_BUFFER_BYTES;

	/**
	 * How many bytes are currently allocated to read buffers.
	 */
	private final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

	public TAsyncServer(Args args) {
		super(args);
		MAX_READ_BUFFER_BYTES = args.maxReadBufferBytes;
		
		this.pendingQueue = (args.pendingQueue == null ? createPendingQueue(args) : args.pendingQueue);
	}

	/**
	 * Begin accepting connections and processing invocations.
	 */
	public void serve() {
		// start listening, or exit
		if (!startListening()) {
			return;
		}

		// start the selector, or exit
		if (!startSelectorThread()) {
			return;
		}

		setServing(true);

		// this will block while we serve
		joinSelector();

		setServing(false);

		// do a little cleanup
		stopListening();
	}
	
	protected static MessageQueue<MessageBuffer> createPendingQueue(Args options) {
		return new MessageQueue<MessageBuffer>();
	}
	
	public MessageQueue<MessageBuffer> getPendingQueue() {
		return this.pendingQueue;
	}

	/**
	 * Have the server transport start accepting connections.
	 * 
	 * @return true if we started listening successfully, false if something
	 *         went wrong.
	 */
	protected boolean startListening() {
		try {
			serverTransport_.listen();
			return true;
		} catch (TTransportException ttx) {
			LOG.error("Failed to start listening on server socket!", ttx);
			return false;
		}
	}

	/**
	 * Stop listening for connections.
	 */
	protected void stopListening() {
		serverTransport_.close();
	}

	/**
	 * Start the selector thread running to deal with clients.
	 * 
	 * @return true if everything went ok, false if we couldn't start for some
	 *         reason.
	 */
	protected boolean startSelectorThread() {
		// start the selector
		try {
			selectThread_ = new SelectThread(
					(TNonblockingServerTransport) serverTransport_, pendingQueue);
			stopped_ = false;
			selectThread_.start();
			return true;
		} catch (IOException e) {
			LOG.error("Failed to start selector thread!", e);
			return false;
		}
	}

	/**
	 * Block until the selector exits.
	 */
	protected void joinSelector() {
		// wait until the selector thread exits
		try {
			selectThread_.join();
		} catch (InterruptedException e) {
			// for now, just silently ignore. technically this means we'll have
			// less of
			// a graceful shutdown as a result.
		}
	}

	/**
	 * Stop serving and shut everything down.
	 */
	public void stop() {
		stopped_ = true;
		if (selectThread_ != null) {
			selectThread_.wakeupSelector();
		}
	}

	public boolean isStopped() {
		return selectThread_.isStopped();
	}
	
	public static interface Stats {
		public int getPendingQueueSize();
		public int getCompletionQueueSize();
	}
	
	public Stats getStats() {
		return new Stats() {
			
			@Override
			public int getPendingQueueSize() {
				return TAsyncServer.this.pendingQueue == null ? -1 : TAsyncServer.this.pendingQueue.size();
			}
			
			@Override
			public int getCompletionQueueSize() {
				return TAsyncServer.this.selectThread_ == null ? -1 : TAsyncServer.this.selectThread_.completeMessages;
			}
		};
	}

	/**
	 * The thread that will be doing all the selecting, managing new connections
	 * and those that still need to be read.
	 */
	protected class SelectThread extends Thread {

		private final TNonblockingServerTransport serverTransport;
		private final Selector selector;

		// List of Connections that want to change their selection interests.
		private final ConcurrentHashMap<Connection, Connection> selectInterestChanges = new ConcurrentHashMap<Connection, Connection>();
		
		private final MessageQueue<MessageBuffer> pendingQueue;
		
		private volatile int completeMessages = 0;

		/**
		 * Set up the SelectorThread.
		 */
		public SelectThread(final TNonblockingServerTransport serverTransport, MessageQueue<MessageBuffer> pendingQueue)
				throws IOException {
			this.serverTransport = serverTransport;
			this.pendingQueue = pendingQueue;
			this.selector = SelectorProvider.provider().openSelector();
			serverTransport.registerSelector(selector);
			setName("TAsyncServer Selector");
		}

		public boolean isStopped() {
			return stopped_;
		}

		/**
		 * The work loop. Handles both selecting (all IO operations) and
		 * managing the selection preferences of all existing connections.
		 */
		public void run() {
			try {
				while (!stopped_) {
					select();
					processInterestChanges();
				}
			} catch (Throwable t) {
				LOG.error("run() exiting due to uncaught error", t);
				try {
					System.err.println("system exit due to fatal error");
					Thread.sleep(1000);
					System.exit(1);		//exit
				} catch (Throwable th) {}
			} finally {
				stopped_ = true;
			}
		}

		/**
		 * If the selector is blocked, wake it up.
		 */
		public void wakeupSelector() {
			selector.wakeup();
		}

		/**
		 * Add Connection to the list of select interest changes and wake up
		 * the selector if it's blocked. When the select() call exits, it'll
		 * give the Connection a chance to change its interests.
		 */
		public void requestSelectInterestChange(Connection conn) {
			selectInterestChanges.putIfAbsent(conn, conn);
			// wakeup the selector, if it's currently blocked.
			selector.wakeup();
		}

		/**
		 * Check to see if there are any Connections that have switched their
		 * interest type of write.
		 */
		private void processInterestChanges() {
			try {
				Iterator<Connection> iter = selectInterestChanges.keySet().iterator();
				int completeMessages = 0;
				for (; iter.hasNext(); ) {
					Connection conn = iter.next();
					iter.remove();
					int size = conn.completionQueue.size();
					if (size > 0) {
						try {
							conn.selectionKey.interestOps(conn.selectionKey.interestOps() | SelectionKey.OP_WRITE);
						} catch (Exception x) {
							LOG.warn(conn + " already closed, of which completion msgs will be discard");
						}
					}
					completeMessages += size;
				}
				this.completeMessages = completeMessages;
			} catch (Exception e) {
				LOG.warn("Got an Exception while processInterestChanges!", e);
			}
		}

		/**
		 * Select and process IO events appropriately: If there are connections
		 * to be accepted, accept them. If there are existing connections with
		 * data waiting to be read, read it, buffering until a whole frame has
		 * been read. If there are any pending responses, buffer them until
		 * their target client is available, and then send the data.
		 */
		private void select() {
			try {
				// wait for io events.
				selector.select();

				// process the io events we received
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> iter = selectedKeys.iterator();
				while (!stopped_ && iter.hasNext()) {
					SelectionKey key = iter.next();
					iter.remove();

					// skip if not valid
					if (!key.isValid()) {
						cleanupSelectionkey(key);
						continue;
					}

					// if the key is marked Accept, then it has to be the server
					// transport.
					if (key.isAcceptable()) {
						handleAccept();
					} else if (key.isReadable()) {
						// deal with reads
						handleRead(key);
					} else if (key.isWritable()) {
						// deal with writes
						handleWrite(key);
					} else {
						LOG.warn("Unexpected state in select! "
								+ key.interestOps());
					}
				}
			} catch (Exception e) {
				LOG.warn("Got an Exception while selecting!", e);
			}
		}

		/**
		 * Accept a new connection.
		 */
		private void handleAccept() throws IOException {
			SelectionKey clientKey = null;
			TNonblockingSocket client = null;
			try {
				// accept the connection
				client = (TNonblockingSocket) serverTransport.accept();
				clientKey = client.registerSelector(selector,
						SelectionKey.OP_READ);

				// add this key to the map
				Connection conn = new Connection(client, clientKey, pendingQueue);
				clientKey.attach(conn);
			} catch (Exception tte) {
				// something went wrong accepting.
				LOG.warn("Exception trying to accept!", tte);
				tte.printStackTrace();
				if (clientKey != null) {
					cleanupSelectionkey(clientKey);
				}
				if (client != null) {
					client.close();
				}
			}
		}

		/**
		 * Do the work required to read from a readable client. 
		 */
		private void handleRead(SelectionKey key) {
			Connection conn = (Connection) key.attachment();
			try {
				conn.readRequest();
			} catch (Exception e) {
				if (!conn.isNewCreated()) {	//suppress disconnect msg due to client heart beat 
					LOG.warn(getName() + " read " + conn + " failed: " + e.getMessage());
				}
				conn.close();
			}
		}

		/**
		 * Let a writable client get written, if there's data to be written.
		 */
		private void handleWrite(SelectionKey key) {
			Connection conn = (Connection) key.attachment();
			try {
				conn.writeResponse();
			} catch (Exception e) {
				LOG.warn(getName() + " write " + conn + " failed: " + e.getMessage());
				conn.close();
			}
		}

	    /**
	     * Do connection-close cleanup on a given SelectionKey.
	     */
	    private void cleanupSelectionkey(SelectionKey key) {
	    	Connection conn = (Connection)key.attachment();
	      if (conn != null) {
	        conn.close();
	      }
	      key.cancel();
	    }

	} // SelectorThread
	
	class Connection {

		// the actual transport hooked up to the client.
		private final TNonblockingSocket trans;
		private final String name;

		// the SelectionKey that corresponds to our transport
		private final SelectionKey selectionKey;
		
		private final MessageQueue<MessageBuffer> pendingQueue;
		private final MessageQueue<MessageBuffer> completionQueue = new MessageQueue<MessageBuffer>();
		
		private final ByteBuffer reqSizeBuffer = ByteBuffer.allocate(4);
		private MessageBuffer reading = null;
		
		private MessageBuffer writing = null;

		private boolean newCreated = true;
		
		Connection(final TNonblockingSocket trans,
				final SelectionKey selectionKey, MessageQueue<MessageBuffer> pendingQueue) {
			this.trans = trans;
			this.selectionKey = selectionKey;
			this.pendingQueue = pendingQueue;
			String name = "Client Connection ";
			try {
				name += trans.getSocketChannel().socket().getRemoteSocketAddress();
			} catch (Exception e) {
				name += trans.getSocketChannel();
			}
			this.name = name;
		}
		
		void addCompleteMsg(MessageBuffer msg) {
			completionQueue.offer(msg);
			selectThread_.requestSelectInterestChange(this);
		}
		
		void readRequest() throws Exception {
			if (reading == null) {					//middle in reading req size
				if (reqSizeBuffer.hasRemaining() && trans.read(reqSizeBuffer) < 0) {
					throw new Exception("EOF");
				}
				if (!reqSizeBuffer.hasRemaining()) {	//just end reading req size
					int reqSize = reqSizeBuffer.getInt(0);
					if (reqSize <= 0) {
						throw new Exception("Read an invalid req size of " + reqSize);
					}
					if (reqSize > MAX_READ_BUFFER_BYTES) {
						throw new Exception("Read a req size of " + reqSize + ", which is bigger than the maximum allowable buffer size for ALL connections.");
					}
					reading = new MessageBuffer(reqSize, this);
				}
			}
			if (reading != null && reading.getReqBuffer().hasRemaining()) {		//middle in reading req body
				if (trans.read(reading.getReqBuffer()) < 0) {
					throw new Exception("EOF");
				}
				if (!reading.getReqBuffer().hasRemaining()) {				//end reading req body
					addPendingMsg(reading);
					reading = null;
					reqSizeBuffer.rewind();
				}
			}
			if (newCreated) {
				newCreated = false;
			}
		}

		void addPendingMsg(MessageBuffer msg) {
			pendingQueue.offer(msg);
		}
		
		void writeResponse() throws Exception {
			if (writing == null) {
				writing = completionQueue.poll();
			}
			
			if (writing != null) {
				if (writing.getRespSizeBuffer().hasRemaining()) {
					trans.write(writing.getRespSizeBuffer());
				}
				if (!writing.getRespSizeBuffer().hasRemaining() && writing.getRespBuffer().hasRemaining()) {	//middle in frame buffer
					trans.write(writing.getRespBuffer());
	    		}
	    		if (!writing.getRespBuffer().hasRemaining()) {
	    			writing = completionQueue.poll();
	    		}
			}
			
			if (writing == null) {				//no responses, need not write
				selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
	    	}
		}
		
		boolean isNewCreated() {
			return newCreated;
		}
		
		void close() {
			try {
				trans.close();
				selectionKey.cancel();
			} catch (Exception ex) {
				LOG.warn("error closing " + this, ex);
			}
			completionQueue.clear();
		}
		
		public String toString() {
			return this.name;
		}
	}
	
	public static class MessageBuffer {
		private final ByteBuffer reqBuffer;
		private ByteBuffer respSizeBuffer;
		private ByteBuffer respBuffer;
		private final Connection conn;
		private String method;
		private int seqid;
		
		MessageBuffer(int size, Connection conn) {
			this.reqBuffer = ByteBuffer.allocate(size);
			this.conn = conn;
		}
		
		/**
		 * used by async server framework
		 * @return
		 */
		ByteBuffer getReqBuffer() {
			return this.reqBuffer;
		}
		
		ByteBuffer getRespBuffer() {
			return this.respBuffer;
		}
		
		ByteBuffer getRespSizeBuffer() {
			return this.respSizeBuffer;
		}

		String getMethod() {
			return method;
		}

		void setMethod(String method) {
			this.method = method;
		}
		
		public int getSeqid() {
			return seqid;
		}

		public void setSeqid(int seqid) {
			this.seqid = seqid;
		}

		/**
		 * used by consumer thread
		 * @return
		 */
		public final byte[] getRequestBuffer() {
			return reqBuffer.array();
		}
		
		public final void setResponseBuffer(byte[] buff, int offset, int len) {
			respBuffer = ByteBuffer.wrap(buff, offset, len);
			respSizeBuffer = ByteBuffer.allocate(4);
			respSizeBuffer.putInt(0, len);
			conn.addCompleteMsg(this);
		}
	}

}
