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
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.server.ServerCommand;

public class InstantTransfer implements ITransfer {

	Logger log = Logger.getLogger(InstantTransfer.class.getName());
	
	InetSocketAddress serverAddr;
	Catalog catalog;
	SocketChannel channel;
	ByteBuffer buf;
	
	
	
	public InstantTransfer(InetSocketAddress serverAddr, Catalog catalog)
			throws IOException {
		this.serverAddr = serverAddr;
		this.catalog = catalog;
		this.channel = SocketChannel.open(serverAddr);
		this.buf = ByteBuffer.allocate(512);
	}

	@Override
	public void transfer(long commitTime, IHolder rowsHolder, UDsHolder upsertsHolder) throws IOException {
		if (rowsHolder.size()>0){
			flushRows(commitTime, rowsHolder);
		} 
		if (upsertsHolder.size()>0){
			flushUDs(commitTime, upsertsHolder);
		}
	}
	
	protected void flushRows(long commitTime, IHolder rowsHolder) throws IOException {
		// Stream format: command (int)|length of tableName(int)|tableName|commitTime (long)|numberofrows(int)|data of rows in rowsHolder
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (4 + 4 + tblBytes.length + 8 + 4 > 512) {
			throw new IOException("Failed to export!");
		}
		buf.clear();
		buf.putInt(ServerCommand.PROD_COMMIT_FLUSH_DATA.ordinal())
				.putInt(tblBytes.length).put(tblBytes).putLong(commitTime)
				.putInt(rowsHolder.size()) // The number of rows
				.flip();
		channel.write(buf);
		rowsHolder.transferTo(channel);
		Utils.checkFailure(channel, "Failed to transfer data to server!");
	}
	
	protected void flushUDs(long commitTime, UDsHolder udsHolder) throws IOException{
		// Even in the lazy commit, the upsert operation will be flushed to the catalyste immediately as
		// some data might already resides in the catalyst which needs to be deleted as well.
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (4 + 4 + tblBytes.length + 8 > 512) {
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
	public void materialize() throws IOException {
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (tblBytes.length + 4 + 4 > 512) {
			throw new IOException("The length of the table name is too long!");
		}
		
		buf.clear();
		buf.putInt(ServerCommand.PROD_COMMIT_MATERIALIZE.ordinal()).putInt(tblBytes.length).put(tblBytes);
		buf.flip();
		channel.write(buf);
		
		int result = Utils.readInt(channel, buf);
		if (result==ServerCommand.ERR.ordinal()){
			throw new IOException("Failed to materialize!");
		}
	}

	@Override
	public void rollback() throws IOException {
		byte[] tblBytes = Utils.getBytesUtf8(catalog.getTableName());
		if (tblBytes.length + 4 + 4 > 512) {
			throw new IOException("Failed to rollback!");
		}
		buf.clear();
		buf.putInt(ServerCommand.PROD_ROLLBACK.ordinal())
				.putInt(tblBytes.length).put(tblBytes);
		buf.flip();
		channel.write(buf);
	}

	@Override
	public void done() throws IOException {
		if (channel!=null && channel.isOpen()){
			Utils.bye(channel, buf);
		}
	}
	
	@Override
	public void ensureAccuracy(long reqCommitTime) throws IOException {
		// no need in the instant flush
	}

	@Override
	public boolean isConnected() throws IOException {
		return channel!=null && channel.isConnected();
		
	}
}
