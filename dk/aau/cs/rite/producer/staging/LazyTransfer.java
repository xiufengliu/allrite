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

package dk.aau.cs.rite.producer.staging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.server.ServerCommand;

public class LazyTransfer implements ITransfer {

	Catalog catalog;
	RowsArchive archive;
	ConcurrentLinkedQueue<ArchiveLog> archiveLogs;
	ExecutorService executor;
	InetSocketAddress serverAddr;
	ByteBuffer buf;
	SocketChannel channel;
	final int BUF_SIZE = 1024;

	public LazyTransfer(InetSocketAddress serverAddr, Catalog catalog) throws IOException {
		this.serverAddr = serverAddr;
		this.catalog = catalog;
		this.archive = new RowsArchive();
		this.archiveLogs = new ConcurrentLinkedQueue<ArchiveLog>();
		this.buf = ByteBuffer.allocate(BUF_SIZE);
		this.channel = SocketChannel.open(serverAddr);
	}

	@Override
	synchronized public void transfer(long commitTime, IHolder rowsHolder, UDsHolder udsHolder) throws IOException {
		if (rowsHolder.size() > 0) {
			ArchiveLog archiveLog = this.archive.archiveRows(commitTime, rowsHolder);
			archiveLogs.add(archiveLog);
		}

		if (udsHolder.size() > 0) {
			flushUDs(commitTime, udsHolder);
		}
	}
	
	protected void flushUDs(long commitTime, UDsHolder udsHolder) throws IOException{
		// Even in the lazy commit, the upsert operation will be flushed to the catalyste immediately as
		// some data might already resides in the catalyst which needs to be deleted as well.
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (4 + 4 + tblBytes.length + 8 > BUF_SIZE) {
			throw new IOException("Failed to export!");
		}
		buf.clear();
		buf.putInt(ServerCommand.PROD_COMMIT_FLUSH_UD.ordinal())
				.putInt(tblBytes.length).put(tblBytes).putLong(commitTime).flip();
		channel.write(buf);
		udsHolder.transferTo(channel);
		Utils.checkFailure(channel, "Failed to transfer the upserts to server!");
	}
	

	@Override
	synchronized public void ensureAccuracy(long reqCommitTime) throws IOException {	
		if (!isConnected()) return;
		
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (4+4+tblBytes.length+8+4 > BUF_SIZE) {
			throw new IOException("The length of file name cannot exceed 256!");
		}
		
		ArchiveLog archiveLog;
		int length = 0;
		int rowCount = 0;
		long commitTime = 0; 
		while ((archiveLog = archiveLogs.peek()) != null
				&& archiveLog.getCommitTime() <= reqCommitTime) {
			length += archiveLog.getLength();
			rowCount += archiveLog.getRowsCount();
			commitTime = archiveLog.getCommitTime();
			archiveLog = archiveLogs.poll();
		}
		
		if (rowCount>0){
			buf.clear();
			buf.putInt(ServerCommand.PROD_COMMIT_FLUSH_DATA.ordinal())
					.putInt(tblBytes.length).put(tblBytes)
					.putLong(commitTime)
					.putInt(rowCount).flip();
			channel.write(buf);
			archive.readTo(channel, length);
			Utils.checkFailure(channel, "Failed to transfer data to server!");
		}
		this.doEmptyUpdate(channel, buf, tblBytes, reqCommitTime);
	}

	private final int doEmptyUpdate(ByteChannel channel, ByteBuffer buf, byte[]tblBytes, long reqCommitTime) throws IOException{
		// To unclock the waiting commitLock in the server.
		buf.clear();
		buf.putInt(ServerCommand.PROD_COMMIT_FLUSH_DATA.ordinal())
				.putInt(tblBytes.length).put(tblBytes).putLong(reqCommitTime)
				.putInt(0) // number of rows
				.flip();
		int written = this.channel.write(buf);
		Utils.checkFailure(channel, "Failed to do empty update!");
		return written;
	}
	
	@Override
	synchronized public void materialize() throws IOException {
		if (!isConnected()) return;
		ensureAccuracy(System.currentTimeMillis());
		archiveLogs.clear();
		
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (4 + 4 + tblBytes.length+8+4 > BUF_SIZE) {
			throw new IOException("The length of file name cannot exceed 256" );
		}
		buf.clear();
		buf.putInt(ServerCommand.PROD_COMMIT_MATERIALIZE.ordinal())
				.putInt(tblBytes.length).put(tblBytes);
		buf.flip();
		channel.write(buf);
		Utils.checkFailure(channel, "Failed to materialize!");
	}

	@Override
	public void rollback() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	synchronized public void done() throws IOException {
		if (channel!=null && channel.isOpen()){
			Utils.bye(channel, buf);
		}
	}

	@Override
	public boolean isConnected() throws IOException {
		return channel!=null && channel.isConnected();
	}
}
