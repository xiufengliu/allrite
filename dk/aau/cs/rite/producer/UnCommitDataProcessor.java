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
package dk.aau.cs.rite.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.producer.staging.IHolder;
import dk.aau.cs.rite.producer.staging.RowsHolder;
import dk.aau.cs.rite.producer.staging.UDsHolder;
import dk.aau.cs.rite.tuplestore.MemBasedTupleStore;
import dk.aau.cs.rite.tuplestore.TupleStore;

public class UnCommitDataProcessor {
	Pipe pipe;
	WritableByteChannel out;
	ReadableByteChannel in;
	ByteBuffer buf;
	Catalog catalog;
	
	TupleStore tmpTupleStore;
	long processTime; 
	
	public UnCommitDataProcessor(Catalog catalog) {
		try {
			this.catalog = catalog;
			pipe = Pipe.open();
			out = pipe.sink();
			in = pipe.source();
			buf = ByteBuffer.allocate(512);
			tmpTupleStore = new MemBasedTupleStore(catalog, 100*1024*1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void writeRows(long commitTime, IHolder rowsHolder) throws IOException {
		// Stream format: command (int)|length of tableName(int)|tableName|commitTime (long)|numberofrows(int)|data of rows in rowsHolder
		if (rowsHolder.size()>0){
			buf.clear();
			buf.putInt(rowsHolder.size()).flip();
			out.write(buf);
			rowsHolder.transferTo(out);
			
			buf.clear();
			tmpTupleStore.readRows(in, buf);
		}
	}
	
	
	protected void writeUDs(long commitTime, UDsHolder udsHolder) throws IOException{
		// Even in the lazy commit, the upsert operation will be flushed to the catalyste immediately as
		// some data might already resides in the catalyst which needs to be deleted as well.
		if (udsHolder.size()>0){
			buf.clear();
			udsHolder.transferTo(out);
			
			buf.clear();
			tmpTupleStore.readUDs(commitTime, in, buf, false);
		}
	}
	
	public void process(IHolder rowsHolder, UDsHolder udsHolder) throws IOException{
		processTime = System.currentTimeMillis();
		this.writeRows(processTime, rowsHolder);
		this.writeUDs(processTime, udsHolder);
	}
	
	public Object[] nextRow() throws IOException{
		tmpTupleStore.writeRowsTo(out, 0, Integer.MAX_VALUE, processTime);
		buf.clear();
		int[] types = catalog.getTypeArray();
		int[] dbtypes = new int[types.length-1];
		System.arraycopy(types, 0, dbtypes, 0, types.length-1);
		return Utils.readRow(in, dbtypes, buf);
		
		
	}
}
