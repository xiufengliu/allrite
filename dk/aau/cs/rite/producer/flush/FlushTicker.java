/*
 *
 * Copyright (c) 2011, Xiufeng Liu (xiliu@cs.aau.dk) and the eGovMon Consortium
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */

package dk.aau.cs.rite.producer.flush;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.producer.staging.ITransfer;
import dk.aau.cs.rite.server.ServerCommand;

public class FlushTicker {

	Logger log = Logger.getLogger(FlushTicker.class.getName());
	Map<String, ITransfer> transfers;
	AtomicBoolean running;
	IFlush flusher;
	InetSocketAddress serverAddr;
	SocketChannel channel;
	
	public FlushTicker(IFlush flusher, InetSocketAddress serverAddr)
			throws IOException {
		this.flusher = flusher;
		this.running = new AtomicBoolean(true);
		this.serverAddr = serverAddr;
		this.transfers = Collections.synchronizedMap(new HashMap<String, ITransfer>());
		this. channel = SocketChannel.open(serverAddr);
		Thread flushThread = new Thread(new Exportor());
		flushThread.start();
	}

	public void addTransfer(String tableName, ITransfer transfer)
			throws IOException {
		// Create pingServer for every tableName.
		
		ByteBuffer buf = ByteBuffer.allocate(1024);
		byte[] tblBytes = Utils.getBytesUtf8(tableName);
		buf.putInt(ServerCommand.PING_CONNECT.ordinal()).putInt(tblBytes.length).put(tblBytes);
		buf.flip();
		channel.write(buf);

		if (Utils.readInt(channel, buf)==ServerCommand.OK.ordinal()) {
			transfers.put(tableName, transfer);
			//Utils.bye(channel, buf);
		} else {
			throw new IOException("Failed to connect the server!");
		}
	}

		
	public void close() throws IOException {
		running.set(false);
	}
	
	class Exportor implements Runnable {
		ByteBuffer buf;

		public Exportor() throws IOException {
			this.buf = ByteBuffer.allocate(1024 * 5);
		}

		@Override
		public void run() {
			try {
				while (running.get() && flusher.flushNow()) {
					 pingAccuracy();
					
					/*ITransfer transfer = transfers.get("orders");
					if (transfer!=null)
						transfer.ensureAccuracy(System.currentTimeMillis());*/
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Utils.bye(channel, buf);
				Utils.closeQuietly(channel);
			}
		}

		private void pingAccuracy() throws IOException {
			if (transfers.size() > 0) {
				buf.clear();
				buf.putInt(ServerCommand.PING_ENSURE_ACCURACY.ordinal())
						.putInt(transfers.size());
				for (String tableName : transfers.keySet()) {
					byte[] tblBytes = Utils.getBytesUtf8(tableName);
					buf.putInt(tblBytes.length).put(tblBytes);
				}
				buf.flip();
				channel.write(buf);
								
				for (int i = 0; i < transfers.size(); ++i) {
					String tableName = Utils.readString(channel);
					long reqCommitTime = Utils.readLong(channel);
					if (reqCommitTime > 0) {
						log.info("Consumer request flush:" + tableName + "," + reqCommitTime);
						ITransfer transfer = transfers.get(tableName);
						if (transfer!=null)
							transfer.ensureAccuracy(reqCommitTime);
					}
				}
			}
		}
	}
}
