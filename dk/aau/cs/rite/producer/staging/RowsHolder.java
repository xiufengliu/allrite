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

import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NOT_NULL;
import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NULL;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Types;
import java.util.Iterator;


 
/**
 * A RowHandler holds rows in a a buffer in the virtual memory until
 * the buffer gets full. Then the buffer's content is automatically written to 
 * a file. When rows are fetched, they appear are returned in the insertion
 * order, and both data from the file and memory buffer are returned.
 */
public class RowsHolder implements Iterator<Object[]>, Iterable<Object[]>, IHolder {

    /** Decides if a new array is constructed when <code>getRow</code> is
     * invoked. @see #getRow()
     */
    public boolean reuseArray = true;
    private final static int DEFAULT_BUFFER_SIZE = 1024*10;
    private final int bSize;
    private ByteBuffer buff;
    private FileChannel fc;
    private int[] types;
    private int mode = WRITE;
    private int lastCnt = -1;
    private Object[] retVals;
    private Charset coder = Charset.forName("UTF-8");
    private int rowCount = 0;
    public static final int READ = 1;
    public static final int WRITE = 2;
    
    
    /** Creates a new instance of RowHandler */
    public RowsHolder(int bufferSize, int[] types) throws IOException {
        bSize = bufferSize;
        buff = ByteBuffer.allocateDirect(bSize);  // Check also with .allocate(bSize)
        File file = File.createTempFile("rite-a", null);
        file.deleteOnExit();
        fc = new RandomAccessFile(file, "rw").getChannel();
        this.types = types;
    }
    
    public RowsHolder(int[] types) throws IOException {
        this(DEFAULT_BUFFER_SIZE,  types);
    }
    

    public FileChannel getFileChannel(){
    	return fc;
    }
    
    /** Returns the current mode (READ or WRITE) of the RowHandler*/
    public int getMode() {
        return mode;
    }
    
    /** Returns the number of rows currently held by this RowHandler*/
    @Override
    public int size() {
        return rowCount;
    }

    /** Puts a row into the RowHandler. The row is given as an Object array 
     * where the type of each Object nad length of the array must match the 
     * type information given when the RowHandler was constructed.
    */
    public void putRow(Object[] values) throws IOException {
        if(mode != WRITE)
            prepareWrite();
        
        for(int i = 0; i < types.length; i++) {
            ensureForWrite(1); // For null flag
            if(values[i] == null) {
                // Write the null flag and go on
                buff.put(IS_NULL);
                continue;
            }
            else buff.put(IS_NOT_NULL);
            
            // We find each type to write to the buffer. Before we write
            // to the buffer, we have to ensure that it has space enough
            // to hold a value of the data type (NB: Strings and thus Dates
            // require special treatment). 
            switch(types[i]) {
                case Types.BIGINT: // i.e. long
                    ensureForWrite(8);
                    buff.putLong((Long)values[i]);
                    break;
                case Types.DATE:
                    // Write it as a string - without any length flag
                    // The length is fixed to 10: YYYY-MM-DD
                    // NB: For a normal string, we would check the length
                    // But here we know that we use UTF-8 and only write numbers
                    // and dashes which all take one byte in UTF-8.
                    ensureForWrite(10);
                    String dateString = ((Date)values[i]).toString();
                    byte[] dateBytes = dateString.getBytes(coder);
                    buff.put(dateBytes, 0, 10);
                    break;
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.NUMERIC:
                    ensureForWrite(8);
                    buff.putDouble((Double)values[i]);
                    break;
                case Types.REAL:
                    ensureForWrite(4);
                    buff.putFloat((Float)values[i]);
                    break;
                case Types.INTEGER:
                    ensureForWrite(4);
                    buff.putInt((Integer)values[i]);
                    break;
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                    // Write the length of the bytes encoded as UTF-8.
                    // Then write the bytes
                    byte[] ascii = ((String)values[i]).getBytes(coder);
                    ensureForWrite(4 + ascii.length); // Length + the bytes
                    buff.putInt(ascii.length).put(ascii, 0, ascii.length);
                    break;
                default:
                    throw new RuntimeException("Unexpected type found");
            }
        }
        rowCount++;
    }

    /** Returns the next row as an Object array or <code>null</code> 
     * if there are no more rows.
     * If <code>reuseArray</code> is <code>true</code>, the Object array will
     * only be created once, but its values overwritten in each call. Otherwise,
     * a new array is created in each invokation.
     */
    public Object[] getRow() throws IOException {
        if(mode != READ)
            prepareRead();
        
        if(buff.remaining() == 0) {
            // All data has been read
            lastCnt = fc.read(buff);
            buff.flip();
        }
        
        if(lastCnt == -1)
            // There is not more data in the file. We only try to read when
            // the buffer is exhausted. So we are out of data...
            return null;
        
        // Now there must be data in the buffer. Start to build up the return
        // values.
        if(!reuseArray || retVals == null)
            retVals = new Object[types.length];
        
        byte[] tmp = new byte[64];
        for(int i = 0; i < types.length; i++) {
            if(isNull()) {
                retVals[i] = null;
                continue;
            }
            
            // We find each type to read from the buffer. Before we read
            // from the buffer, we have to ensure that it has bytes enough
            // to read a value of the data type (NB: Strings and thus Dates
            // require special treatment). Otherwise we compact it, fill it
            // (the two latter are handled in the ensureForRead method), 
            // and then read.
            switch(types[i]) {
                case Types.BIGINT: // i.e. long
                    ensureForRead(8);
                    retVals[i] = buff.getLong();
                    break;
                case Types.DATE:
                    // Read it as a string - without any length flag
                    // The length is fixed to 10: YYYY-MM-DD
                    ensureForRead(10);
                    buff.get(tmp, 0, 10);
                    retVals[i] = Date.valueOf(new String(tmp, 0, 10, coder));
                    break;
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.NUMERIC:
                    ensureForRead(8);
                    retVals[i] = buff.getDouble();
                    break;
                case Types.REAL:
                    ensureForRead(4);
                    retVals[i] = buff.getFloat();
                    break;
                case Types.INTEGER:
                    ensureForRead(4);
                    retVals[i] = buff.getInt();
                    break;
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                    ensureForRead(4);
                    int length = buff.getInt();
                    ensureForRead(length);
                    byte[] ascii;
                    if(length < 64) // Reuse tmp as often as possible
                        ascii = tmp;
                    else
                        ascii = new byte[length]; // Make a new temporary then
                    buff.get(tmp, 0, length);
                    retVals[i] = new String(ascii, 0, length, coder);
                    break;
                default:
                    throw new RuntimeException("Unexpected type found");
            }
        }
        
        return retVals;
    }
        
    /** Ensures that we can read the given number of bytes from the buffer*/
    private final void ensureForRead(int bytes) throws IOException {
        if(mode != READ)
            throw new IllegalStateException("Must be in read mode");
        if(buff.remaining() < bytes) {
            buff.compact();
            lastCnt = fc.read(buff);
            buff.flip();
        }
        if(buff.remaining() < bytes)
            throw new BufferUnderflowException();
    }
    
    /** Ensures that there us space in the buffer to read the given number of
     * bytes. */
    private final void ensureForWrite(int bytes) throws IOException {
        if(mode != WRITE)
            throw new IllegalStateException("Must be in write mode");
        if(buff.remaining() < bytes) {
            // Write out to file
            buff.flip();
            fc.write(buff);
            buff.clear();
        }
        if(buff.remaining() < bytes)
            throw new BufferOverflowException();
    }
    
    /** Reads the null flag and returns whether the flag indicates a NULL 
     * value.*/
    private final boolean isNull() throws IOException {
        ensureForRead(1); // Make sure that we have data
        byte val = buff.get();
        return (val == IS_NULL);
    }

    /** Puts the RowHander into READ mode such that rows can be fetched. */
    public void prepareRead() throws IOException {
        if(mode == READ)
            return;
        if(fc.size() > 0) {
            // There is already data in the file. Write the buffer and read
            buff.flip();
            fc.write(buff);
            buff.clear();
            fc.position(0);
            lastCnt = fc.read(buff);
            buff.flip();
        }
        else {
            // The buffer has not be emtied to the file yet.
            // We can just read directly from the buffer now.
            buff.flip();
            lastCnt = buff.remaining(); // A trick to make it look like a read
        }
        mode = READ;
    }
    
    /** Puts the RowHandler into WRITE mode such that rows can be added. */
    public void prepareWrite() throws IOException {
        if(mode == WRITE)
            return;
        buff.clear();
        fc.position(fc.size());
        mode = WRITE;
    }
    
    /** Drops all rows held by the RowHandler */
    public void clear() throws IOException {
        if(mode != WRITE)
            prepareWrite();
        buff.clear();
        fc.truncate(0);
        fc.position(0); // Needed because of Java bug 6191269
        rowCount = 0;
        lastCnt = -1; // Indicate that we have not read anything
    }
    
    /** Writes all rows held by the RowHanlder to its underlying file. */
    public void force() throws IOException {
        if(mode != WRITE)
            throw new IllegalStateException("Can only force writing in write mode");
        buff.flip();
        fc.write(buff);
        fc.force(false);
        buff.clear();
    }
    
    /** Returns the current byte position for the RowHandler. */
    public long position() throws IOException {
        if(mode == READ)
            return fc.position() + buff.position();
        else // i.e. WRITE
            return fc.size() + buff.position();
    }
        
    /** Moves the RowHandler to a specific byte position. Can only be done
     * when the RowHandler is in READ mode
     */
    public void move(long pos) throws IOException {
        if(mode != READ)
            throw new IllegalStateException("Must be in read mode");
        if(pos > fc.size() + buff.limit() || pos < 0)
            throw new IllegalArgumentException("Bad position: " + pos);
        
        if(position() == pos)
            return;
        
        if(pos < fc.size()) {
            // The position is in the file. Read it in.
            fc.position(pos);
            buff.clear();
            lastCnt = fc.read(buff);
            buff.flip();
        }
        else { //I.e. pos >= fc.size() and pos  is somewhere in the buffer
            buff.position((int)(pos - fc.size()));
        }
        return;
    }
        
    /** Transfers all this RowHandler's byte data to the given ByteChannel.*/
    @Override
    public long transferTo(WritableByteChannel dest) throws IOException {        
        if(mode == WRITE)
            force(); // Write all data to fc
        
        long remaining = fc.size();
        long length = remaining;
        long from = 0;
        while(remaining > 0) {
            long cnt = fc.transferTo(from, remaining, dest);
            remaining -= cnt;
            from += cnt;
        }
      return length;
    }
    
    
	@Override
	public void read(ReadableByteChannel src) throws IOException {
		while (src.read(buff)!=-1) {
			buff.flip();
			fc.write(buff);
			buff.clear();
		}
	}
    
    
    /** Returns an Iterator over the rows held by this RowHandler. */
    @Override
    public Iterator<Object[]> iterator() {
        try {
            prepareRead();
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
    
    /** Tells whether there are more rows to fetch. */
    @Override
    public boolean hasNext() {
        if(mode != READ)
            throw new IllegalStateException("Must be in read mode");
        
        return (!(buff.remaining() == 0 && lastCnt == -1));
    }

    /** Returns the next row held by this RowHandler. */
    @Override
    public Object[] next() {
        // next is here to implement Iterator
        try {
            return getRow();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    
    /** Unsupported operation! Will thrown an UnsupportedOperationException.*/
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove rows!");
    }

    
	public String toString(){
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("RowsHolder's rowCount=").append(this.rowCount).append("\n");
		return strBuilder.toString();
	}
}
