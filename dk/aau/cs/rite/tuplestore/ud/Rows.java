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

package dk.aau.cs.rite.tuplestore.ud;

import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NULL;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Types;
import java.util.Iterator;

public class Rows implements Iterator<Object[]> {

	int rowID;

	String[] cols;
	int[] types;
	Object[] retVals;
	ByteBuffer src;
	ReadableByteChannel in;

	public Rows(ByteBuffer src, String[] cols, int[] types) {
		this.src = src;
		this.cols = cols;
		this.types = types;
	}

	public void setValues(Object[] retVals) {
		this.retVals = retVals;
	}

	public void setValues(ByteBuffer buf) {
		this.src = buf;
	}

	@Override
	public boolean hasNext() {
		return src.remaining() != 0;
	}

	@Override
	public Object[] next() {
		try {
			return getRow();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {

	}

	private Object[] getRow() throws IOException {
		Object[] retVals = new Object[types.length];
		
		//retVals[types.length-1] = src.getInt(); // The last object in retVals array is the rowID
		byte[] tmp = new byte[64];
		for (int i = 0; i < types.length; ++i) {
			if (src.get() == IS_NULL) {
				retVals[i] = null;
				continue;
			}
			switch (types[i]) {
			case Types.BIGINT: // i.e. long
				retVals[i] = src.getLong();
				break;
			case Types.DATE:
				src.get(tmp, 0, 10);
				retVals[i] = Date.valueOf(new String(tmp, 0, 10, Charset.forName("UTF-8")));
				break;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
				retVals[i] = src.getDouble();
				break;
			case Types.REAL:
				retVals[i] = src.getFloat();
				break;
			case Types.INTEGER:
				retVals[i] = src.getInt();
				break;
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				int length = src.getInt();
				byte[] ascii;
				if (length < 64) // Reuse tmp as often as possible
					ascii = tmp;
				else
					ascii = new byte[length]; // Make a new temporary then
				src.get(ascii, 0, length);
				retVals[i] = new String(ascii, 0, length, Charset.forName("UTF-8"));
				break;
			default:
				throw new RuntimeException("Unexpected type found");
			}
		}
		return retVals;
	}
	
	
	private Object[] getRowFromChannel() throws IOException {
		Object[] retVals = new Object[types.length];
		
		//retVals[types.length-1] = src.getInt(); // The last object in retVals array is the rowID
		byte[] tmp = new byte[64];
		for (int i = 0; i < types.length; ++i) {
			if (src.get() == IS_NULL) {
				retVals[i] = null;
				continue;
			}
			switch (types[i]) {
			case Types.BIGINT: // i.e. long
				retVals[i] = src.getLong();
				break;
			case Types.DATE:
				src.get(tmp, 0, 10);
				retVals[i] = Date.valueOf(new String(tmp, 0, 10, Charset.forName("UTF-8")));
				break;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
				retVals[i] = src.getDouble();
				break;
			case Types.REAL:
				retVals[i] = src.getFloat();
				break;
			case Types.INTEGER:
				retVals[i] = src.getInt();
				break;
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				int length = src.getInt();
				byte[] ascii;
				if (length < 64) // Reuse tmp as often as possible
					ascii = tmp;
				else
					ascii = new byte[length]; // Make a new temporary then
				src.get(ascii, 0, length);
				retVals[i] = new String(ascii, 0, length, Charset.forName("UTF-8"));
				break;
			default:
				throw new RuntimeException("Unexpected type found");
			}
		}
		return retVals;
	}
	
}
