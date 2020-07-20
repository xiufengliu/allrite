/*
 *
 * Copyright (c) 2007, Christian Thomsen (chr@cs.aau.dk) and the EIAO Consortium
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
 * Created on January 19, 2007, 3:29 PM
 *
 *
 */


package dk.aau.cs.rite.tuplestore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.logging.Logger;

import dk.aau.cs.rite.server.RiteChannel;

/**
 * 
 * @author chr
 */
public class Segment { // package access only
	
	Logger log = Logger.getLogger(Segment.class.getName());
	
	private int indexGrowthSize;

	private int used = 0;
	private int firstRowID = Integer.MIN_VALUE;
	private int lastRowID = Integer.MIN_VALUE;
	private int rowCount = 0;
	private byte[] data;
	private int[] rowIDs;
	private int[] positions;

	public Segment() {
		this(16 * 1024 * 1024);
	}

	public Segment(int size) {
		this(size, 32);
	}

	public Segment(int size, int avgRowSize) {
		data = new byte[size];
		indexGrowthSize = size / avgRowSize;
		rowIDs = new int[indexGrowthSize];
		positions = new int[indexGrowthSize];

	}

	private Segment(int used, int firstRowID, int lastRowID, int rowCount,
			byte[] data, int[] rowIDs, int[] positions) {
		this.used = used;
		this.firstRowID = firstRowID;
		this.lastRowID = lastRowID;
		this.rowCount = rowCount;
		this.data = data;
		this.rowIDs = rowIDs;
		this.positions = positions;
	}

	public int remaining() {
		return data.length - used;
	}


    public int addRow(byte[] row, int rowLength, int rowID) {
        if(rowCount == rowIDs.length)
            expandIndex();
        rowIDs[rowCount] = rowID;
        positions[rowCount] = used;
        System.arraycopy(row, 0, data, used, rowLength);
        if(rowCount == 0)
            firstRowID = rowID;
        lastRowID = rowID;
        rowCount++;
        used += rowLength;
        
        return positions[rowCount-1]; //return the position of the row just added.
    }
	
    
    
    
	private void expandIndex() {
		rowIDs = Arrays.copyOf(rowIDs, rowIDs.length + indexGrowthSize);
		positions = Arrays.copyOf(positions, positions.length + indexGrowthSize);
	}

	public int[] getLimits(int from, int to) {
		// Assume from <= to.
		// We find the positions of the first byte and the last byte that
		// hold data for rows with IDs in between from and to (both incl.)
		// If the segment only holds data between from and to, we return
		// everything. If the segment has data that is not within the interwal,
		// we must find the the part to return.
		// We do this by performing binary searches in rowIDs[].
		// First, we search between position 0 and rowCount for the smallest
		// element e1 such that from <= e1. If we don't find such an element,
		// this segment has no relevant data (and we return null).
		// If we found an e1 at position pos1, the first byte to include is
		// given by positions[pos1]. We then continue to do a
		// binary search for the largest element e2 such that e2 <= to.
		// It is enough to search between [pos1, rowCount].
		// We will for sure find an element (since e1 exists). Let pos2
		// be its position. If pos2 == rowCount - 1, the last byte to return
		// is given by used - 1. Else, the last byte to return is
		// given by positions[pos2 + 1] - 1,i.e., the last byte before the
		// first not to include... :-)
		if (from > to)
			throw new IllegalArgumentException("from > to");

		if (rowCount == 0)
			return null;

		if (firstRowID >= from && lastRowID <= to)
			return new int[] { 0, used - 1 };

		int pos1 = doBinarySearch(0, rowCount, from, true);
		if (pos1 == rowCount)
			return null;

		int pos2 = doBinarySearch(pos1, rowCount, to, false);

		return new int[] { positions[pos1],
				(pos2 == rowCount - 1 ? used : positions[pos2 + 1]) - 1 };
	}

	/**
	 * Uses Arrays.binarySearch(..) to search in rowIDs[]. If the result r of
	 * binarySearch is non-negative, it is returned directly. Else if !include,
	 * a negative result r from binarySearch is returned as: -(r+2) (i.e., the
	 * position before the place where the key would be inserted is returned).
	 * Else (when include == true), a negative result r from binarySearch is
	 * returned as: -(r+1) (i.e., the position where the key would be inserted
	 * is returned).
	 */
	private int doBinarySearch(int fromPos, int toPos, int key, boolean include) {
		int res = Arrays.binarySearch(rowIDs, fromPos, toPos, key);
		if (res < 0) {
			if (include)
				res = -(res + 1);
			else
				res = -(res + 2);
		}
		return res;
	}

	/**
	 * Removes rows with and ID greater than or equal to the given.
	 * 
	 * @return true if the Segment still has data after the deletion, false
	 *         otherwise.
	 */
	public boolean dropFrom(int fromRowID) { // This is for rollback.
		if (rowCount == 0)
			return false;
		if (lastRowID < fromRowID)
			return true; // we know that rowCount > 0 so there is data

		int pos = doBinarySearch(0, rowCount, fromRowID, true);
		// pos now holds the position in rowIDs from which we should delete
		// rows.
		// We set used = positions[pos].
		// Further, we must set rowIDs[x] = 0 for x \in {pos, ..., rowCount-1}
		// and likewise for positions.
		used = positions[pos];
		for (int i = pos; i < rowCount; i++) {
			rowIDs[i] = 0;
			positions[i] = 0;
		}
		rowCount = pos;
		this.rowIDs = rowIDs;
		return (pos == 0);
	}

	public int transferData(WritableByteChannel dest, int fromRow, int toRow) throws IOException {
		int[] limits = getLimits(fromRow, toRow);
		if (limits == null)
			return -1;
		ByteBuffer bb = ByteBuffer.wrap(data, limits[0], limits[1] - limits[0] + 1);
		dest.write(bb);
		return limits[0]; // Return a row's starting position the data segment.
	}

	public int readRow(WritableByteChannel dest, int rowID) throws IOException{
		return transferData(dest, rowID, rowID);
	}
	
	public void replaceRow(byte[]row, int rowLength, int destPos){
		System.arraycopy(row, 0, data, destPos, rowLength);
	}
	
	public void transferData(WritableByteChannel dest) throws IOException {
		this.transferData(dest, firstRowID, lastRowID);
	}
	
	public int getFirstRowID() {
		return firstRowID;
	}

	public int getLastRowID() {
		return lastRowID;
	}

	public int used() {
		return used;
	}

	/** Returns the number of bytes needed to store this Segment. */
	public long serializedSize() {
		// see writeOut below
		long res = 24; // 6 ints in the beginning
		res += used; // data
		res += 2 * 4 * rowCount; // rowIDs and positions
		return res;
	}

	/** Returns the segment size */
	public int size() {
		return data.length;
	}

	public void writeOut(WritableByteChannel dest) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(24);
		bb.putInt(used).putInt(firstRowID).putInt(lastRowID).putInt(rowCount).putInt(data.length).putInt(rowIDs.length);
		bb.flip();
		dest.write(bb);
		bb = ByteBuffer.wrap(data);
		bb.limit(used);
		dest.write(bb);
		// Now we have to write out the int arrays rowIDs and positions.
		// Unfortunately, we cannot just give the int arrays to the
		// WritableByteChannel so we have to create byte arrays that duplicate
		// the data... :-(
		byte[] copy = new byte[4 * rowCount];
		bb = ByteBuffer.wrap(copy);
		for (int i = 0; i < rowCount; i++)
			bb.putInt(rowIDs[i]);
		bb.flip();
		dest.write(bb);
		// We don't have to reset copy as rowIDs and positions have the same
		// number of used/unused elements
		bb.clear();
		for (int i = 0; i < rowCount; i++)
			bb.putInt(positions[i]);
		bb.flip();
		dest.write(bb);
	}

	public static Segment readIn(ReadableByteChannel src) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(24);
		readFromChannel(src, bb);
		int used = bb.getInt();
		int firstRowID = bb.getInt();
		int lastRowID = bb.getInt();
		int rowCount = bb.getInt();
		int datalen = bb.getInt();
		int idxlen = bb.getInt();
		byte[] data = new byte[datalen];
		bb = ByteBuffer.wrap(data);
		bb.limit(used);
		readFromChannel(src, bb);
		byte[] tmp = new byte[4 * rowCount];
		bb = ByteBuffer.wrap(tmp);
		readFromChannel(src, bb);
		int[] rowIDs = new int[idxlen];
		int[] positions = new int[idxlen];
		bb.asIntBuffer().get(rowIDs, 0, rowCount);
		bb.clear();
		readFromChannel(src, bb);
		bb.asIntBuffer().get(positions, 0, rowCount);

		return new Segment(used, firstRowID, lastRowID, rowCount, data, rowIDs, positions);
	}

	private static void readFromChannel(ReadableByteChannel src, ByteBuffer dest)
			throws IOException {
		while (dest.remaining() > 0) {
			int tmp = src.read(dest);
			if (tmp == -1)
				throw new IOException("Unexpected EOF");
		}
		dest.flip();
	}

	
	public void materialize(RiteChannel dest, String delim, String nullSubst) throws IOException {
		ByteBuffer bb = ByteBuffer.wrap(data, 0, used);
		dest.write(bb, delim, nullSubst);
	}


	
}
