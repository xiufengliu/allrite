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

import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NOT_NULL;
import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NULL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.server.PingServer;
import dk.aau.cs.rite.server.RiteChannel;
import dk.aau.cs.rite.sqlparser.SQLType;
import dk.aau.cs.rite.tuplestore.ud.AlgRel;
import dk.aau.cs.rite.tuplestore.ud.Delete;
import dk.aau.cs.rite.tuplestore.ud.Expression;
import dk.aau.cs.rite.tuplestore.ud.Operation;
import dk.aau.cs.rite.tuplestore.ud.UDStore;
import dk.aau.cs.rite.tuplestore.ud.Update;

public class FileBasedTupleStore implements TupleStore, Observer {
	Logger log = Logger.getLogger(FileBasedTupleStore.class.getName());
	FlushEvent flushEvent = new FlushEvent();
	
	private UDStore udStore; // Used to store the updates and deletes.
	private Catalog catalog;
	private PingServer pingServer;

	private String segFilePattern;
	private int segmentSize;
	private int segmentID;
	private  int ID; 
	private Segment curSegment;
	private int []types;
	private byte[] tmpRow;
	private int lastMatRowID; // last materialized rowID which is used for updating the min value in the minMax table.
	
	
	LocalMaterializer localMaterializer;
	
	private SortedMap<Long, Locker> reqLockers =  Collections.synchronizedSortedMap(new TreeMap<Long, Locker>());
	
	// (commitTime -> maxRowID in a commit (include))
	private ReadWriteMap<Long, Integer> timeIndex = new ReadWriteMap<Long, Integer>();

	// (minRowID(in a segment) -> counter)
	private ReadWriteMap<Integer, Counter> registerCounter = new ReadWriteMap<Integer, Counter>();

	// (minRowID(in a segment) -> segmentID)
	private ReadWriteMap<Integer, Integer> rowIndex = new ReadWriteMap<Integer, Integer>();
	
	// (segmentID -> a segment). The segment might be not in the mem, then read in from the backed file.
	private ReadWriteMap<Integer, SoftReference<Segment>> segCache = new ReadWriteMap<Integer, SoftReference<Segment>>();

	 public FileBasedTupleStore(String tupleStoreDir, Catalog catalog, int segmentSize) throws RiteException  {
		this.catalog = catalog;
		this.udStore = new UDStore(catalog, flushEvent);
		this.types = catalog.getTypeArray();
		
		this.localMaterializer = new LocalMaterializer(tupleStoreDir);
		
		this.segFilePattern = new StringBuilder(tupleStoreDir).append(File.separator).append("seg%d.dat").toString();
		this.ID = 0;
		this.segmentID = 0;
		this.segmentSize = segmentSize;
		this.tmpRow = new byte[4096];
		this.lastMatRowID = -1;
		
		this.flushEvent.addObserver(this);
		this.flushEvent.addObserver(timeIndex);
		this.flushEvent.addObserver(registerCounter);
		this.flushEvent.addObserver(rowIndex);
		this.flushEvent.addObserver(segCache);
	}

	@Override
	public void setPingServer(PingServer pingServer) {
		this.pingServer = pingServer;
	}

	protected Segment getSegment(int segmentID) throws IOException {
		SoftReference<Segment> sr = segCache.getFromRMap(segmentID);
		Segment seg = sr.get();
		if (seg == null) {
			String fileName = String.format(segFilePattern, segmentID);
			File dest = new File(fileName);
			RandomAccessFile raf = new RandomAccessFile(dest, "r");
			FileChannel channel = raf.getChannel();
			seg = Segment.readIn(channel);
			raf.close();
			sr = new SoftReference<Segment>(seg);
			segCache.putInRMap(segmentID, sr);
		}
		return seg;
	}
	
	@Override
	public int readRowsIn(ReadableByteChannel channel) throws RiteException{
		try {
			ByteBuffer buf = ByteBuffer.allocate(16384);
			buf.flip();
			int read = Utils.ensureForRead(channel, buf, 8);
			long commitTime = buf.getLong();
			int numOfRows = this.readRows(channel, buf);
			if (numOfRows > 0) {
				timeIndex.put(commitTime, ID);
				flushEvent.end();
				updateMinMax();
			}
			this.resumeTheWaitingReadThreads(commitTime);
			return numOfRows;
		} catch (Exception e) {
			e.printStackTrace();
			flushEvent.fail();
			throw new RiteException(e);
		}
	}
	
	final protected void resumeTheWaitingReadThreads(long commitTime) {
		if (pingServer != null) {
			long reqCommitTime = 0;
			while (!reqLockers.isEmpty() && (reqCommitTime = reqLockers.firstKey()) <= commitTime) {
				Locker cond = reqLockers.get(reqCommitTime);
				if (cond != null) {
					cond.signalFlush();
					reqLockers.remove(reqCommitTime);
				}
			}
		}
	}
	
	@Override
	public int readUDsIn(ReadableByteChannel channel) throws RiteException {
		try {
			ByteBuffer buf = ByteBuffer.allocate(16384);
			buf.flip();
			int read = Utils.ensureForRead(channel, buf, 8);
			long commitTime = buf.getLong();
			int numOfUDs = this.readUDs(commitTime, channel, buf, true);
			if (numOfUDs>0){
				flushEvent.end();
			}
			return numOfUDs;
		} catch (Exception e) {
			e.printStackTrace();
			flushEvent.fail();
			throw new RiteException(e);
		}
	}
	
	@Override
	public int readRows(ReadableByteChannel channel, ByteBuffer buf) throws IOException{
		int read = Utils.ensureForRead(channel, buf, 4);
		int numOfRows = buf.getInt();
		if (numOfRows>0){
			flushEvent.start();
			shrink();   //For persistence, 2012.04.24
		}
		
		int n = numOfRows;
		int rowLength = 0;
		while (n-- > 0) {
			rowLength = 0;
			for (int i = 0; i < types.length; i++) {
				read = Utils.ensureForRead(channel, buf, 1);
				Utils.copyBytes(buf, tmpRow, rowLength, 1);
				rowLength += 1;
				// Check if the last written byte (the flag) indicated NULL
				if (tmpRow[rowLength - 1] == IS_NULL)
					continue;
				if (tmpRow[rowLength - 1] != IS_NOT_NULL)
					throw new IOException("Illegal flag value");

				switch (types[i]) {
				case Types.BIGINT: // i.e. long
					read = Utils.ensureForRead(channel, buf, 8);
					Utils.copyBytes(buf, tmpRow, rowLength, 8);
					rowLength += 8;
					break;
				case Types.DATE:
					// Read it as a string - without any length flag
					// The length is fixed to 10: YYYY-MM-DD
					read = Utils.ensureForRead(channel, buf, 10);
					Utils.copyBytes(buf, tmpRow, rowLength, 10);
					rowLength += 10;
					break;
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.NUMERIC:
					read = Utils.ensureForRead(channel, buf, 8);
					Utils.copyBytes(buf, tmpRow, rowLength, 8);
					rowLength += 8;
					break;
				case Types.REAL:
					read = Utils.ensureForRead(channel, buf, 4);
					Utils.copyBytes(buf, tmpRow, rowLength, 4);
					rowLength += 4;
					break;
				case Types.INTEGER:
					read = Utils.ensureForRead(channel, buf, 4);
					Utils.copyBytes(buf, tmpRow, rowLength, 4);
					rowLength += 4;
					break;
				case Types.LONGVARCHAR:
				case Types.VARCHAR:
					/*
					 * // Code to use if all strings have a length //
					 * smaller than the SIZE of the input buffer. lastCnt =
					 * ensureForRead(input, channel, 4); int length =
					 * input.getInt(); input.position(input.position() - 4);
					 * // Read length lastCnt = ensureForRead(input,
					 * channel, length + 4); copyBytesToBuff(input, length +
					 * 4);
					 */
					read = Utils.ensureForRead(channel, buf, 4);
					int length = buf.getInt();
					// Also read the length field when
					// reading again:
					buf.position(buf.position() - 4);
					length += 4;
					int toRead;
					toRead = Math.min(buf.capacity(), length);
					while (length > 0) {
						read = Utils.ensureForRead(channel, buf, toRead);
						Utils.copyBytes(buf, tmpRow, rowLength, toRead);
						rowLength += toRead;
						length -= toRead;
						toRead = Math.min(buf.capacity(), length);
					}
					break;
				} /* switch */
			} /* for */
			
			ID = Utils.byteArrayToInt(tmpRow, rowLength-4); // The last colvalue is the row ID.
			addRowToSegment(tmpRow, rowLength);
		}/* while */
		
		if (buf.remaining() != 0 || n>0) {
			throw new IOException("The data does not give the expected number of rows");
		}
		return numOfRows;
	}
		
	protected void addRowToSegment(byte[] rowBytes, int rowLength) throws IOException {
		boolean isSegDumped = false; 
		if (curSegment != null && curSegment.remaining() < rowLength) {
			this.dumpCurrentSegment();
			isSegDumped = true;
		}

		if (curSegment == null || isSegDumped) {
			curSegment = new Segment(segmentSize);
			segCache.put(segmentID, new SoftReference<Segment>(curSegment));
			rowIndex.put(ID, segmentID);
			registerCounter.put(ID, new Counter());
		}
		curSegment.addRow(tmpRow, rowLength, ID);
	}
	
	protected void dumpCurrentSegment() throws IOException { // will increase the segmentID for the next flush
		if (curSegment != null) {
			String fileName = String.format(segFilePattern, segmentID);
			File dest = new File(fileName);
			RandomAccessFile raf = new RandomAccessFile(dest, "rw");
			FileChannel channel = raf.getChannel();
			channel.truncate(0); // if the file existed from a prev crash
			curSegment.writeOut(channel);
			raf.close();
			//dest.deleteOnExit();
			++segmentID;
			curSegment = null;
		}
	}
	
	public int readUDs(long commitTime, ReadableByteChannel channel, ByteBuffer buf, boolean delRowsInDw) throws IOException { // Save the UDATE/DELETE operations in the UDStore.
		//1.Read the number of UD
		Utils.ensureForRead(channel, buf, 4);
		int numOfUDs = buf.getInt();
		if (numOfUDs>0){
			flushEvent.start();
			if (!rowIndex.isEmpty())
				udStore.shrink(this.rowIndex.firstKey());
		}
		
		for (int i = 0; i < numOfUDs; ++i) {

			//2.Read type: UPDATE or DELETE
			Utils.ensureForRead(channel, buf, 4);
			int opType = buf.getInt();
			
			//3. Read the ID
			Utils.ensureForRead(channel, buf, 4);
			this.ID = buf.getInt();
			
			//4.Read SQL statement
			Utils.ensureForRead(channel, buf, 4);
			int l = buf.getInt();
			byte[] dst = new byte[l];
			Utils.ensureForRead(channel, buf, l);
			buf.get(dst);
			String sql = Utils.getStringFromUtf8(dst);
			
			
			Map<Integer, Expression> exps = this.readExps(channel, buf);			
			List<Integer> params = this.readIntArr(channel, buf);
			List<Integer> conds = this.readIntArr(channel, buf);
			
			List<Expression> wheres = new ArrayList<Expression>();
			for (int j=0; j<conds.size(); ++j){
				int id = conds.get(j);
				wheres.add(exps.get(id));
			}
			
			Operation ud = null;
			if (opType == SQLType.UPDATE.ordinal()) {
				List<Expression> updates = new ArrayList<Expression>();
				for (int id: exps.keySet()){
					if (!conds.contains(id)){
						updates.add(exps.get(id));
					}
				}
				ud = new Update(updates, wheres.size() == 0 ? null : wheres.get(0));
			} else if (opType == SQLType.DELETE.ordinal()){
				ud = new Delete(wheres.size() == 0 ? null : wheres.get(0)); // Currently, supports only one where condition.
			} else {
				throw new IOException(String.format("Unknown operation type, %d\n", opType));
			}
			if (delRowsInDw)
				this.executeUDForDW(sql, exps, params); // Delete or Update the table in DW.
			ud.setID(this.ID); // Set the unique ID.
			udStore.add(ud);
		}
		
		if (numOfUDs>0){// Add the timeIndex for udStore.
			udStore.put(commitTime, this.ID);
		}
		
		if (buf.remaining() != 0) {
			throw new IOException("The data for upserts was corrupted!");
		}
		return numOfUDs;
	}

	final protected List<Integer> readIntArr(ReadableByteChannel channel, ByteBuffer buf)throws IOException {
		List<Integer> indice = new ArrayList<Integer>();
		Utils.ensureForRead(channel, buf, 4);
		int count = buf.getInt();
		Utils.ensureForRead(channel, buf, 4 * count);
		for (int i = 0; i < count; ++i) {
			indice.add(buf.getInt());
		}
		return indice;
	}
	
	final protected Map<Integer, Expression> readExps(ReadableByteChannel channel, ByteBuffer buf) throws IOException {
		Map<Integer, Expression> exps = new HashMap<Integer, Expression>();
		Utils.ensureForRead(channel, buf, 4);
		int numOfExps = buf.getInt();
		for (int j = 0; j < numOfExps; ++j) {
			Utils.ensureForRead(channel, buf, 8);
			int colId = buf.getInt();
			int relId = buf.getInt();
			Expression exp = new Expression(colId, AlgRel.values()[relId], null);

			Object val = null;
			Utils.ensureForRead(channel, buf, 1);
			if (buf.get() == IS_NOT_NULL) {
				switch (types[colId]) {
				case Types.BIGINT: // i.e. long
					Utils.ensureForRead(channel, buf, 8);
					val = buf.getLong();
					break;
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.NUMERIC:
					Utils.ensureForRead(channel, buf, 8);
					val = buf.getDouble();
					break;
				case Types.REAL:
					Utils.ensureForRead(channel, buf, 4);
					val = buf.getFloat();
					break;
				case Types.INTEGER:
					Utils.ensureForRead(channel, buf, 4);
					val = buf.getInt();
					break;
				case Types.DATE:
					Utils.ensureForRead(channel, buf, 4);
					int l = buf.getInt();
					byte[] dst = new byte[l];
					Utils.ensureForRead(channel, buf, l);
					buf.get(dst);
					val = Date.valueOf(Utils.getStringFromUtf8(dst));
					break;
				case Types.LONGVARCHAR:
				case Types.VARCHAR:
					Utils.ensureForRead(channel, buf, 4);
					l = buf.getInt();
					dst = new byte[l];
					Utils.ensureForRead(channel, buf, l);
					buf.get(dst);
					val = Utils.getStringFromUtf8(dst);
					break;
				default:
					throw new RuntimeException("Unexpected type found");
				}
			}
			exp.setVal(val);
			exps.put(colId, exp);
		}
		return exps;
	}
	
	@Override
	public boolean register(int minRowID, int maxRowID) {
	if (pingServer == null) {
		try {
			NavigableMap<Integer, Counter> registers = registerCounter.subMap(minRowID, false, maxRowID, false);
			log.info(String.format(	"Register %s: min=%d, max=%d, match=%d\n",catalog.getTableName(), minRowID, maxRowID, registers.size()));
			for (Integer rowID : registers.keySet()) {
				Counter counter = registers.get(rowID);
				counter.inc();
			}
		}catch(Exception e){
			return false;
		} 
	}
	return true;
	}

	@Override
	public void unregister(int minRowID, int maxRowID) {
		if (pingServer==null){
			NavigableMap<Integer, Counter> registers = registerCounter.subMap(minRowID, false, maxRowID, false);
			log.info(String.format("UNREGISTER %s: min=%d, max=%d, match=%d\n", catalog.getTableName(), minRowID, maxRowID, registers.size()));
			for (Integer rowID : registers.keySet()) {
				Counter counter = registers.get(rowID);
				counter.dec();
			}
		}
	}
	
	protected void ensureAccuracy(WritableByteChannel dest, long queryStartTime, long freshness) throws IOException {
		long reqCommitTime = queryStartTime - freshness;
		log.info(String.format("%s requests rows committed before %d\n", catalog.getTableName(), reqCommitTime));
		if (timeIndex.isEmpty() || timeIndex.lastKey() < reqCommitTime) {
			pingServer.setTimeAccuracy(catalog.getTableName(), reqCommitTime);
			Locker cond = new Locker();
			reqLockers.put(reqCommitTime, cond);
			cond.awaitFlush();
		}
		
		Entry<Long, Integer> entry = timeIndex.floorEntry(reqCommitTime);
		log.info(String.format("Righttime: sending rows %s  reqCommitTime=%d\n", catalog.getTableName(), reqCommitTime));
		if (entry!=null) {
			int maxRowID = entry.getValue();
			NavigableMap<Integer, Integer> toBeExported = rowIndex.headMap(maxRowID, true);
			for (int segmentID : toBeExported.values()) {
				Segment segment = this.getSegment(segmentID);
				segment.transferData(new RiteChannel(dest, queryStartTime, udStore));
			}
		}
		
	}
	

	
	protected void materialize(BufferedWriter dest, String delim, String nullSubst) throws IOException {
		this.dumpCurrentSegment();
		NavigableMap<Integer, Integer> toBeMatted = rowIndex.tailMap(lastMatRowID, false);
		for (Entry<Integer, Integer> entry : toBeMatted.entrySet()) {
			int rowID = entry.getKey();
			Counter counter = this.registerCounter.get(rowID);
			if (!counter.isMaterialized()) {
				int segmentID = entry.getValue();
				Segment segment = this.getSegment(segmentID);
				segment.materialize(new RiteChannel(dest, System.currentTimeMillis(), udStore), delim, nullSubst);
				counter.markMaterialized();
				lastMatRowID = segment.getLastRowID();
			}
		}
		this.curSegment = null;
	}


	@Override
	public void writeRowsTo(WritableByteChannel dest, int minRowID, int maxRowID, long queryStartTime) throws IOException {
		NavigableMap<Integer, Integer> toBeExported = this.rowIndex.subMap(minRowID, false, maxRowID, false);
		log.info(String.format("Realtime: sending rows %s: min=%d, max=%d, matches=%d\n", catalog.getTableName(), minRowID, maxRowID, toBeExported.size()));
		for (int segmentID : toBeExported.values()) {
			Segment segment = this.getSegment(segmentID);
			segment.transferData(new RiteChannel(dest, queryStartTime, udStore), minRowID, maxRowID);
		}
		
	}	
	
	@Override
	public void query(WritableByteChannel dest, int minRowID, int maxRowID, long queryStartTime, long reqFreshness) throws IOException {
		if (pingServer!=null){//The producer is using lazy flush
			ensureAccuracy(dest, queryStartTime, reqFreshness);
		} else {// Using instant flush
			writeRowsTo(dest, minRowID, maxRowID, queryStartTime);
		}
		signalEndOfStream(dest);
	}
	
		
	    protected final ByteBuffer eos  = ByteBuffer.wrap(new byte[]{FlagValues.END_OF_STREAM});
	    protected final void signalEndOfStream(WritableByteChannel dest) throws IOException {
			eos.clear();
			// Safe - sets position to 0, and limit to 1
			dest.write(eos);
	    }
		
		protected void shrink() {
			Entry<Integer, Counter> entry;
			NavigableMap<Integer, Counter> regs = registerCounter.getWritableMap();
			while ((entry = regs.firstEntry()) != null && entry.getValue().canBeDeleted()) {
				int rowID = entry.getKey();
				regs.pollFirstEntry();
				int segmentID = rowIndex.getWritableMap().get(rowID);
				String fileName = String.format(segFilePattern, segmentID);
				File segFile = new File(fileName);
				segFile.delete();
				segCache.getWritableMap().remove(segmentID);
				rowIndex.getWritableMap().remove(rowID);
				NavigableMap<Long, Integer> tIdx = timeIndex.getWritableMap();
				while (tIdx.size() > 0 && tIdx.firstEntry().getValue() < rowID) {
					tIdx.pollFirstEntry();
				}
			}
		}

		@Override
		public int[] getMinMax(){
			return new int[]{lastMatRowID, ID};
		}
		

		@Override
		public Catalog getCatalog() {
			return catalog;
		}
		
		protected void executeUDForDW(String sql, Map<Integer, Expression> exps, List<Integer> params) throws IOException {
			try {
				Connection jdbcConn = this.getJDBCConnection();
				PreparedStatement pstmt = jdbcConn.prepareStatement(sql);
				for (int i=0; i<params.size(); ++i){
					int id = params.get(i);
					Expression exp = exps.get(id);
					pstmt.setObject(i+1, exp.getVal());
				}
				pstmt.execute();
				pstmt.close();
			} catch (Exception e){
				e.printStackTrace();
				throw new IOException(e.getCause());
			} 
		}
		
		@Override
		public void materialize() throws RiteException{
		try{
			String tableName = catalog.getTableName();
			Connection jdbcConn = this.getJDBCConnection();
			File file = File.createTempFile(tableName, ".csv");
			String filePath = file.getCanonicalPath();
			//FileChannel fc = new FileOutputStream(file).getChannel();
			//PrintStream fc = new PrintStream(new BufferedOutputStream(new BackgroundOutputStream(new FileOutputStream(file))));
			BufferedWriter  fc = new BufferedWriter(new FileWriter(file), 8192*2);
			
			String delim = "|";
			String nullStr = "";
			this.materialize(fc, delim, nullStr);
			fc.close();

				
			//Statement stmt = jdbcConn.createStatement();
			//stmt.execute("SET search_path TO dw");
			//stmt.close();

			StringBuilder sqlBuilder = new StringBuilder("COPY ")
					.append(tableName).append(" FROM '").append(filePath)
					.append("' ").append("DELIMITER '").append(delim)
					.append("' NULL '").append(nullStr).append("' CSV");
			PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			pstmt.execute();
			pstmt.close();

			
			sqlBuilder.setLength(0);
			sqlBuilder.append("UPDATE rite_minmax SET min=?, max=? WHERE name=?");
			PreparedStatement pstmtForMinmax = jdbcConn.prepareStatement(sqlBuilder.toString());
			int[]minMax = this.getMinMax();
			pstmtForMinmax.setInt(1, minMax[0]);
			pstmtForMinmax.setInt(2, minMax[1]);
			pstmtForMinmax.setString(3, tableName);
			pstmtForMinmax.execute();
			pstmtForMinmax.close();

			file.delete();
		} catch (IOException ioe){
			throw new RiteException(ioe);
		} catch (SQLException e) {
			throw new RiteException(e);
		}
	}
	
	protected void updateMinMax() throws Exception {
		try{
		Connection jdbcConn = this.getJDBCConnection();
		String sql = "UPDATE rite_minmax SET min=?, max=? WHERE name=?";
		
		StringBuilder sqlBuilder = new StringBuilder(sql);
		PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
		int[] minMax = this.getMinMax();
		
		pstmt.setInt(1, minMax[0]);
		pstmt.setInt(2, minMax[1]);
		pstmt.setString(3, catalog.getTableName());
		pstmt.execute();
		pstmt.close();
		} catch (Exception e){
			e.printStackTrace();
			throw e;
		}
	}
	
	
	protected Connection jdbcConnection = null;
	public Connection getJDBCConnection() throws RiteException {
		try {
			if (jdbcConnection == null) {
				Class.forName(catalog.getJdbcDriver());
				jdbcConnection = DriverManager.getConnection(
						catalog.getJdbcUrl(), catalog.getDbUsername(),
						catalog.getDbPassword());
			}
			return jdbcConnection;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RiteException(e.getMessage());
		}
	}
	
	@Override
	public void close(){
		Utils.closeQuietly(jdbcConnection);
	}

	@Override
	public int getID() {
		return ID; 
	}
	

	protected void backup(){
		try {
			localMaterializer.materializeOthers(segmentSize, segmentID, ID, lastMatRowID);
			this.dumpCurrentSegment();
			localMaterializer.materializeRegCounter(registerCounter);
			localMaterializer.materializeSegCache(segCache);
			localMaterializer.materializeTimeIndex(timeIndex);
			localMaterializer.materializeRowIndex(rowIndex);
			localMaterializer.materializeUDStore(udStore);
			localMaterializer.finalize();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void rollback(){
		try {
			localMaterializer.rollbackRegCounter(registerCounter);
			if (!registerCounter.isEmpty()){
				localMaterializer.rollbackSegCache(segCache);
				localMaterializer.rollbackTimeIndex(timeIndex);
				localMaterializer.rollbackRowIndex(rowIndex);
				localMaterializer.rollbackUDStore(udStore);
				int []others = localMaterializer.rollbackOthers();
				this.segmentSize = others[0];
				this.segmentID = others[1];
				this.ID = others[2];
				this.lastMatRowID = others[3];
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void update(Observable o, Object arg) {
		switch ((FlushEvent.event) arg) {
		case FLUSH_SUCCESS:
			this.backup();
			break;
		case FLUSH_FAIL:
			rollback();
			break;
		default:
			;
		}
	} 
}
