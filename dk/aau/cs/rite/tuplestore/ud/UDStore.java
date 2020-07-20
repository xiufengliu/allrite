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

import java.io.BufferedWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.tuplestore.FlushEvent;
import dk.aau.cs.rite.tuplestore.Persistence;
import dk.aau.cs.rite.tuplestore.ReadWriteDeepMap;
import dk.aau.cs.rite.tuplestore.ReadWriteMap;

public class UDStore implements Persistence{// Update/Delete operation store, U represents UPDATE, and D represents DELETE.
	
	private Catalog catalog;
	
	//ID - > UD object 
	private ReadWriteMap<Integer, Operation> udNoIdxMap = new ReadWriteMap<Integer, Operation> ();
	
	// The value of a column with index -> List of UD objects (Currently only supports a indexed column in a table).
	private ReadWriteDeepMap udWithIdxMap = new ReadWriteDeepMap ();
	
	// (commitTime -> max ID of uds in a commit (include))
	private ReadWriteMap<Long, Integer> timeIndex = new ReadWriteMap<Long, Integer>();
	
	
	public UDStore(Catalog catalog,  FlushEvent flushEvent){
		this.catalog = catalog;
		flushEvent.addObserver(udNoIdxMap);
		flushEvent.addObserver(udWithIdxMap);
		flushEvent.addObserver(timeIndex);
	}
	
	public int process(ByteBuffer src, String delim, String nullSubst, long queryStartTime, BufferedWriter writer) throws java.io.IOException {
		return this.processRows(src, delim, nullSubst, queryStartTime, null, writer);
	}
	
	public int process(ByteBuffer src, long queryStartTime, WritableByteChannel dst) throws java.io.IOException {
		//return this.processRows(src, null, null, queryStartTime, dst, null);  // Apply on-the-fly updates and deletions to the records.
		return dst.write(src); // Query all the records directly to the consumer side without being processed.
	}

	public int process(ByteBuffer src, String delim, String nullSubst, long queryStartTime, WritableByteChannel dst) throws java.io.IOException {
		return this.processRows(src, delim, nullSubst, queryStartTime, dst, null);
	}
	
	
	private int processRows(ByteBuffer src, String delim, String nullSubst, long queryStartTime, WritableByteChannel dst, BufferedWriter writer) throws java.io.IOException {
			int bytesOfWritten = 0;
			ByteBuffer buf = ByteBuffer.allocate(1024 * 50);
			String[] cols = catalog.getColArray();
			int[] types = catalog.getTypeArray(); // Attention!!!: Type already contains the last ID column, therefore, cols.length+1==types.length
			int idx = catalog.getIndexCol(); // Indexed column in a table.
			Rows rows = new Rows(src, cols, types);
			StringBuffer strBuf = new StringBuffer();
			while (rows.hasNext()) {
				Object[] row = rows.next();
				int rowID = (Integer) row[types.length-1]; // The last column stores the row ID
				
				NavigableMap<Integer, Operation> uds = udNoIdxMap.tailMap(rowID, false);
				
				if (idx>=0){ // Currently, only support one indexed column.
					Object iVal = row[idx]; // The value in the indexed column.
					List<Object> udObjs = udWithIdxMap.get(iVal);
					if (udObjs != null) { // Add the ud objects created on the indexed column.
						for (Object obj : udObjs) {
							Operation ud = (Operation) obj;
							uds.put(ud.getID(), ud);
						}
					}
				}
					
				Entry<Long, Integer> entry = timeIndex.floorEntry(queryStartTime);
				int maxID = entry==null?-1:entry.getValue();
				
				NavigableMap<Integer, Operation>  appliedUds = uds.headMap(maxID, true);
				if (appliedUds!=null){
					for (Entry<Integer, Operation> udEntry : appliedUds.entrySet()) {
						int ID = udEntry.getKey();
						Operation ud = udEntry.getValue();
						if (ud.getID()>rowID){
							row = ud.process(row);
						} else
							break;
					}
				}
	
				if (delim != null && nullSubst != null) { // Materialize
					if (writer != null) {
						Utils.writeRow(writer, strBuf, row, types, delim, nullSubst);
					} else {
						bytesOfWritten += Utils.writeRow(dst, row, types, delim, nullSubst, buf);
						buf.flip();
						dst.write(buf);
						buf.clear();
					}
				} else { // Query
					bytesOfWritten += Utils.writeStream(dst, row, types, buf);
					buf.flip();
					dst.write(buf);
					buf.clear();
				}
		}
	 return bytesOfWritten;
}
	
	



	
	public void put(long commitTime, int ID){
		this.timeIndex.put(commitTime, ID);
	}
		
	public void add(Operation ud) {
		Expression whereExp = ud.getWhereExp();
		// Only supports one indexed-column,\e.g., the primary key.
		if (whereExp != null && catalog.getIndexCol() == whereExp.getCol() && whereExp.getRel()==AlgRel.EqualTo) {
			udWithIdxMap.put(whereExp.getVal(), ud);
			return;
		}
		udNoIdxMap.put(ud.getID(), ud);
	}
	
	
	public void shrink(int minRowID) { 
		//TODO: remove the un-used objects from the udWithIdxMap.
		udNoIdxMap.getWritableMap().headMap(minRowID, false).clear();
		/*Map<Object, List<Object>> idxMap = udWithIdxMap.getWritableMap();
		if (idxMap.size() > 0) {
			for (Entry<Object, List<Object>> entry : idxMap.entrySet()) {
				List<Object> uds = entry.getValue();
				
			}
		}*/
	}

	@Override
	public void backup(String path) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void recover() {
		// TODO Auto-generated method stub
		
	}
}
