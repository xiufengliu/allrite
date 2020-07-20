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

package dk.aau.cs.rite.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import dk.aau.cs.rite.tuplestore.ud.UDStore;

public class RiteChannel implements WritableByteChannel {

	WritableByteChannel channel;
	BufferedWriter writer;
	UDStore udStore;
	long queryStartTime;

	public RiteChannel(WritableByteChannel channel, long queryStartTime, UDStore udStore) {
		this.channel = channel;
		this.udStore = udStore;
		this.queryStartTime = queryStartTime;
	}
	
	public RiteChannel(BufferedWriter writer, long queryStartTime, UDStore udStore) {
		this.writer = writer;
		this.udStore = udStore;
		this.queryStartTime = queryStartTime;
	}
	

	public WritableByteChannel wrap(WritableByteChannel channel) {
		this.channel = channel;
		return this;
	}

	@Override
	public boolean isOpen() {
		return channel.isOpen();
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		return udStore.process(src, queryStartTime, channel);
	}
	
	public int write(ByteBuffer src, String delim, String nullSubst) throws IOException {
		//return udStore.process(src, delim, nullSubst, queryStartTime, channel);
		return udStore.process(src, delim, nullSubst, queryStartTime, writer);
	}
}
