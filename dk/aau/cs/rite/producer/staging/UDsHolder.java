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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.tuplestore.ud.AlgRel;
import dk.aau.cs.rite.tuplestore.ud.Execute;
import dk.aau.cs.rite.tuplestore.ud.Execution;
import dk.aau.cs.rite.tuplestore.ud.Expression;
import dk.aau.cs.rite.tuplestore.ud.Visitor;

public class UDsHolder implements  Visitor{
		
	List<Execute> ops = new ArrayList<Execute>();
	Catalog catalog;
	ByteBuffer buf;
	long bytesWritten;
	WritableByteChannel dest;
	
	public UDsHolder(Catalog catalog){
		this.catalog = catalog;
		this.buf = ByteBuffer.allocate(1024);
	}
	
	public void add(Execute op){
		ops.add(op);
	}
	

	

	
	public long transferTo(WritableByteChannel dest) throws IOException {
		this.dest = dest;
		buf.clear();
		bytesWritten = 0;
		buf.putInt(ops.size()); // Write the total number of Deletes and Updates
		for (Execute op : ops){
			op.accept(this);
		}	
		
		buf.flip();
		bytesWritten += dest.write(buf);
		return bytesWritten;
	}
	

	@Override
	public void visit(Execution op) throws IOException {
		bytesWritten += Utils.ensureForWrite(dest, buf, 8);
		buf.putInt(op.getType().ordinal()).putInt(op.getID());
		
		byte[] tmp = Utils.getBytesUtf8((String) op.getSQL());
		bytesWritten += Utils.ensureForWrite(dest, buf, 4 + tmp.length);
		buf.putInt(tmp.length).put(tmp);

		List<Expression> exps = op.getExps();
		this.writeExps(dest, exps);
		
		int[] params = op.getParams();
		int count = params==null?0:params.length;
		bytesWritten += Utils.ensureForWrite(dest, buf, 4+4*count);
		buf.putInt(count);
		for (int i=0; i<count;++i){
			buf.putInt(params[i]);
		}
		
		List<Integer> conds = op.getConds();
		int size = conds.size();
		bytesWritten += Utils.ensureForWrite(dest, buf, 4+4*size);
		buf.putInt(size);
		for (int i=0; i<size;++i){
			buf.putInt(conds.get(i));
		}
	}
	
	protected final void writeExps(WritableByteChannel dest, List<Expression> exps) throws IOException {
		bytesWritten += Utils.ensureForWrite(dest, buf, 4);
		buf.putInt(exps.size()); // denote the number of Exps
		
		int[] types = catalog.getTypeArray();
		for (Expression exp : exps) {
			int colId = exp.getCol();
			AlgRel rel = exp.getRel();
			Object val = exp.getVal();
			
			bytesWritten += Utils.ensureForWrite(dest, buf, 4 + 4 + 1);
			buf.putInt(colId).putInt(rel.ordinal());
			
			if (val == null) {
				buf.put(IS_NULL);
				continue;
			} else {
				buf.put(IS_NOT_NULL);
			}

			switch (types[colId]) {
			case Types.BIGINT: // i.e. long
				bytesWritten += Utils.ensureForWrite(dest, buf, 8);
				buf.putLong((Long) val);
				break;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
				bytesWritten += Utils.ensureForWrite(dest, buf, 8);
				buf.putDouble((Double) val);
				break;
			case Types.REAL:
				bytesWritten += Utils.ensureForWrite(dest, buf, 4);
				buf.putFloat((Float) val);
				break;
			case Types.INTEGER:
				bytesWritten += Utils.ensureForWrite(dest, buf, 4);
				buf.putInt((Integer) val);
				break;
			case Types.DATE:
                 byte[] tmp = Utils.getBytesUtf8(((Date)val).toString());
                 bytesWritten += Utils.ensureForWrite(dest, buf, 4 + tmp.length);
                 buf.putInt(tmp.length).put(tmp);
                 break;
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				tmp = Utils.getBytesUtf8((String) val);
				bytesWritten += Utils.ensureForWrite(dest, buf, 4 + tmp.length);
				buf.putInt(tmp.length).put(tmp);
				break;
			default:
				throw new RuntimeException("Unexpected type found");
			}
		}
	}

	public void clear() {
		ops.clear();
	}

	
	public int size() {
		return ops.size();
	}
}
