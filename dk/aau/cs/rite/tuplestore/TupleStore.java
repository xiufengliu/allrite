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

package dk.aau.cs.rite.tuplestore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.server.PingServer;

/**
 * 
 * @author liu
 */
public interface TupleStore  {

	Catalog getCatalog();

	void materialize() throws RiteException;

	int readRowsIn(ReadableByteChannel channel) throws RiteException;

	public int readRows(ReadableByteChannel channel, ByteBuffer buf) throws IOException;

	int readUDsIn(ReadableByteChannel channel) throws RiteException;

	public int readUDs(long commitTime, ReadableByteChannel channel, ByteBuffer buf, boolean delRowsInDw)
			throws IOException;

	boolean register(int minRowID, int maxRowID);

	void query(WritableByteChannel dst, int minRowID, int maxRowID,
			long queryStartTime, long reqFreshness) throws IOException;

	void unregister(int minRowID, int maxRowID);
	
	void writeRowsTo(WritableByteChannel dest, int minRowID, int maxRowID, long queryStartTime) throws IOException; 


	void setPingServer(PingServer pingServer);

	int getID();

	int[] getMinMax();

	void close();

	void rollback();
}
