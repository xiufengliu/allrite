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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;



/**
 * A RowArchive is a RowTransferrer that holds its rows in a temporary file.
 * Concretely, it is used for committed rows that have not been transferred to
 * the memory server yet.
 */

/**
 * A RowArchive is a RowTransferrer that holds its rows in a temporary file.
 * Concretely, it is used for committed rows that have not been transferred to
 * the memory server yet.
 */
public class RowsArchive  {
	protected File file;
	protected FileChannel fc;
	protected int rowCount = 0;
	protected long lastTransferedPosition = 0;
	public long time = -1;
	public String name;
	

	/** Creates a new instance of RowArchive */
	public RowsArchive() {
		try {
			file = File.createTempFile("rite-a", null);
			file.deleteOnExit();
			RandomAccessFile data = new RandomAccessFile(file, "rw");
			fc = data.getChannel();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Adds the rows from <code>rh</code> to this archive. <code>rh</code> may
	 * not be modified while this is done. This method does NOT clear
	 * <code>rh</code> afterwards.
	 */
	public ArchiveLog archiveRows(long archiveTime, IHolder rowsHolder) throws IOException {
		long position = fc.position();
		int rowCountOfRowsHolder = rowsHolder.size(); // The number of rows 
		rowCount += rowCountOfRowsHolder;
		long length = rowsHolder.transferTo(fc);
		return new ArchiveLog(archiveTime, position, length, rowCountOfRowsHolder);
	}
	
	
	
	
	public void readTo(WritableByteChannel dest, long count) throws IOException{// Transfer data from archive file to the server.
		fc.transferTo(lastTransferedPosition, count, dest);
		this.lastTransferedPosition += count;
	}
	
	
	public int size() {
		return rowCount;
	}

	/** Transfers all this RowArchive's byte data to the given ByteChannel. */

	/**
	 * Drops this archive and all the rows it holds.
	 */
	public void delete() {
		try {
			fc.close();
			file.delete();
		} catch (IOException e) {
			// We cannot do a lot about it here...
		}
	}

	
	public long transferTo(WritableByteChannel dest) throws IOException {
		long remaining = fc.size();
		long from = 0;
		while (remaining > 0) {
			long cnt = fc.transferTo(from, remaining, dest);
			remaining -= cnt;
			from += cnt;
		}
		return fc.size();
		
	}



	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		return strBuilder.toString();
	}

}
