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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Types;

import dk.aau.cs.rite.common.CircularByteBuffer;


 
/**
 * A RowHandler holds rows in a a buffer in the virtual memory until
 * the buffer gets full. Then the buffer's content is automatically written to 
 * a file. When rows are fetched, they appear are returned in the insertion
 * order, and both data from the file and memory buffer are returned.
 */
public class MemRowsHolder implements  IHolder {

    /** Decides if a new array is constructed when <code>getRow</code> is
     * invoked. @see #getRow()
     */
    public boolean reuseArray = true;
    private final static int DEFAULT_BUFFER_SIZE = 1024*32;
    private final int bSize;
    private ByteBuffer buff;
    private CircularByteBuffer cbb;

    WritableByteChannel outChn;
    ReadableByteChannel inChn;
    
    private int[] types;
    private Charset coder = Charset.forName("UTF-8");
    private int rowCount = 0;
   
    
    /** Creates a new instance of RowHandler */
    public MemRowsHolder(int bufferSize, int[] types) throws IOException {
        bSize = bufferSize;
        buff = ByteBuffer.allocateDirect(bSize);  // Check also with .allocate(bSize)
        this.types = types;
        cbb = new CircularByteBuffer(CircularByteBuffer.INFINITE_SIZE, false);
        outChn = Channels.newChannel(cbb.getOutputStream());
        inChn = Channels.newChannel(cbb.getInputStream());
        
    }
    
    public MemRowsHolder(int[] types) throws IOException {
        this(DEFAULT_BUFFER_SIZE,  types);
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
   

    /** Ensures that there us space in the buffer to read the given number of
     * bytes. */
    private final void ensureForWrite(int bytes) throws IOException {
        if(buff.remaining() < bytes) {
            // Write out to file
            buff.flip();
            outChn.write(buff);
            buff.clear();
        }
        
        if(buff.remaining() < bytes)
            throw new BufferOverflowException();
    }
    


    /** Drops all rows held by the RowHandler */
    public void clear() throws IOException {
      cbb.clear();
      rowCount = 0;
    }
    
    /** Writes all rows held by the RowHanlder to its underlying file. */
    public void force() throws IOException {
        buff.flip();
        outChn.write(buff);
    }
    

    /** Transfers all this RowHandler's byte data to the given ByteChannel.*/
	@Override
	public long transferTo(WritableByteChannel dest) throws IOException {
		force();
		buff.clear();
		long length = 0;
		while ((inChn.read(buff) != -1) || buff.position() > 0) {
			buff.flip();
			length+=dest.write(buff);
			buff.compact();
		}
		return length;
		//return cbb.getAvailable();
	}
    
    
	@Override
	public void read(ReadableByteChannel src) throws IOException {
		
	}
    
    
    
	public String toString(){
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("RowsHolder's rowCount=").append(this.rowCount).append("\n");
		return strBuilder.toString();
	}
}
