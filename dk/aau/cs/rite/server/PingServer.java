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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import dk.aau.cs.rite.common.Utils;

public class PingServer {

	long accurancy;
	ByteBuffer buf;

	public PingServer() {
		this.buf = ByteBuffer.allocate(512 + 4 + 8);
		this.accurancy = Long.MIN_VALUE;
	}

	synchronized public void setTimeAccuracy(String tableName, long commitTime) {
		if (accurancy < commitTime) {
			accurancy = commitTime;
		}
	}

	synchronized public void update(ByteChannel channel, String tableName)
			throws IOException {
		byte[] tblBytes = Utils.getBytesUtf8(tableName);
		if (tblBytes.length > 512) {
			throw new IOException(
					"The lenght of table name should not execeed 256!");
		}
		buf.clear();
		buf.putInt(tblBytes.length).put(tblBytes).putLong(accurancy);
		buf.flip();
		while (channel.write(buf) > 0)
			;

		this.accurancy = Long.MIN_VALUE;
	}
}
