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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.server.ServerCommand;
import dk.aau.cs.rite.tuplestore.Persistence;

public class Catalog  {

	private String tableName;
	private Map<String, Integer> colTypes = new HashMap<String, Integer>();
	private List<String> cols = new ArrayList<String>(); 
	private List<String> primaryKeys = new ArrayList<String>();
	private String jdbcDriver;
	private String jdbcUrl;
	private String dbUsername;
	private String dbPassword;

	private int indexCol = -1;
	private int seq = 0; //The sequence used for rows, updates and deletes.  
	
	private String dataDir;
	
	public Catalog(String tableName, String dataDir) {
		this.tableName = tableName;
		this.dataDir = dataDir;
	}
	

	public Catalog(String tableName) {
		this(tableName, null);
	}


	public String getTableName() {
		return tableName;
	}

	public void addColType(String col, int type) {
		colTypes.put(col, type);
		cols.add(col);
	}

	public int getType(String col) {
		return colTypes.get(col);
	}

	public int indexOf(String col){
		return cols.indexOf(col);
	}
	
	public void syncWithCatalyst(InetSocketAddress serverAddr) throws IOException {
		SocketChannel channel = SocketChannel.open(serverAddr);
		
		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
		byte[] ascii = Utils.getBytesUtf8(tableName);// Set the tablename
		buf.putInt(ServerCommand.PROD_SYNC_CATALOG.ordinal()).putInt(ascii.length).put(ascii);

		buf.putInt(colTypes.size()); // Set number of columns
		for (String col : cols) {
			ascii = Utils.getBytesUtf8(col);// Set the column name
			int colType = colTypes.get(col);// Set the column type
			buf.putInt(ascii.length).put(ascii).putInt(colType);
		}

		buf.putInt(primaryKeys.size()); // Set the number of primary keys
		for (String prmKey : primaryKeys) {
			ascii = Utils.getBytesUtf8(prmKey);// Set the primary key
			buf.putInt(ascii.length).put(ascii);
		}

		ascii = Utils.getBytesUtf8(jdbcDriver);
		buf.putInt(ascii.length).put(ascii);

		ascii = Utils.getBytesUtf8(jdbcUrl);
		buf.putInt(ascii.length).put(ascii);

		ascii = Utils.getBytesUtf8(dbUsername);
		buf.putInt(ascii.length).put(ascii);

		ascii = Utils.getBytesUtf8(dbPassword);
		buf.putInt(ascii.length).put(ascii);
		buf.flip();
		channel.write(buf);
		
		buf.clear();
		Utils.read(channel, 8, buf);
		buf.flip();
		this.seq = buf.getInt(); // Get the current seq in the catalyst tuplestore.
		int succeed = buf.getInt();
		
		if (succeed!=ServerCommand.OK.ordinal()) {
			throw new IOException("Failed to sync catalog to server!");
		}
		Utils.bye(channel, buf);
	}
	
	public int getNextSeq(){
		return ++seq;
	}
	

	
	public void readIn(ByteChannel channel) throws IOException{
		int n = Utils.readInt(channel); // the number of column
		for (int i = 0; i < n; ++i) {
			String col = Utils.readString(channel);
			int type = Utils.readInt(channel); // The type of column
			this.addColType(col, type);
		}
		n = Utils.readInt(channel); // the number of primary keys
		for (int i = 0; i < n; ++i) {
			String primKey = Utils.readString(channel);
			primaryKeys.add(primKey);
			if (i==0){ // Only support the first column of the primary keys with index.
				indexCol = cols.indexOf(primKey);
			}
		}
		jdbcDriver = Utils.readString(channel);
		jdbcUrl = Utils.readString(channel);
		dbUsername = Utils.readString(channel);
		dbPassword = Utils.readString(channel);
	}
	
	public void readButNotSave(ByteChannel channel) throws IOException{
		int n = Utils.readInt(channel); // the number of column
		for (int i = 0; i < n; ++i) {
			Utils.readString(channel);
			Utils.readInt(channel); // The type of column
		}
		n = Utils.readInt(channel); // the number of primary keys
		for (int i = 0; i < n; ++i) {
			Utils.readString(channel);
		}
		Utils.readString(channel);
		Utils.readString(channel);
		Utils.readString(channel);
		Utils.readString(channel);
	}
	
	
	public void addPrimarykey(String keyCol) {
		primaryKeys.add(keyCol);
	}

	public String[] getPrimaryKeys() {
		return primaryKeys.toArray(new String[] {});
	}

	
	public int getIndexCol(){ // Only support one column with index.
		return this.indexCol;
	}
	
	public List<String> getColList(){
		return cols;
	}
	
	public String[] getColArray(){
		return cols.toArray(new String[]{});
	}
	
	public int[] getTypeArray(){
		int[] types = new int[cols.size() + 1];
		for (int i=0; i<cols.size(); ++i){
			types[i] = colTypes.get(cols.get(i));
		}
		types[cols.size()]  = java.sql.Types.INTEGER; // this is left for save the rowID.
		return types;
	}

	public int getType(int pos){
		return this.getType(cols.get(pos));
	}

	public String getCol(int pos){
		return cols.get(pos);
	}
	
	public boolean isKey(int pos){
		return primaryKeys.contains(getCol(pos));
	}
	
	public void setTargetJdbcInfo(String jdbcDriver, String jdbcUrl,
			String dbUsername, String dbPassword) {
		this.jdbcDriver = jdbcDriver;
		this.jdbcUrl = jdbcUrl;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public String getDbUsername() {
		return dbUsername;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String toString() {
		StringBuilder strBld = new StringBuilder();
		strBld.append("tableName=").append(tableName).append("\n");
		
		strBld.append("colTypes=");
		for (int i=0; i<cols.size(); ++i) {
			String col = cols.get(i);			
			String type = String.valueOf(colTypes.get(col));
			strBld.append(col).append(",").append(type);
			if (i!=cols.size()-1) strBld.append(";");
		}
		strBld.append("\n");
		
		strBld.append("primaryKeys=");
		for (int i=0; i<this.primaryKeys.size(); ++i){
			strBld.append(primaryKeys.get(i));
			if (i!=primaryKeys.size()-1)
			 strBld.append(",");
		}
		strBld.append("\n");
		
		strBld.append("jdbcDriver=").append(jdbcDriver).append("\n");
		strBld.append("jdbcUrl=").append(jdbcUrl).append("\n");
		strBld.append("dbUsername=").append(dbUsername).append("\n");
		strBld.append("dbPassword=").append(dbPassword).append("\n");
		strBld.append("indexCol=").append(indexCol).append("\n");
		return strBld.toString();
	}

	public void backup() throws RiteException {
		Writer out = null;
		try {
			out = new OutputStreamWriter(new FileOutputStream(this.dataDir
					+ File.separator + this.tableName+File.separator+"catalog"));
			out.write(this.toString());
		} catch (Exception e) {
			throw new RiteException(e);
		} finally {
			try {
				out.close();
			} catch (Exception e) {
			}
		}
	}

	public void rollback() throws RiteException {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new FileInputStream(this.dataDir
					+ File.separator + this.tableName+File.separator+"catalog"));
		 
			this.tableName = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		  
		    // Read the columns and types;
			cols.clear();
			colTypes.clear();
			String str = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		    String[] colTypes = StringUtils.splitToArray(str, ";", false);
		    for (int i=0; i<colTypes.length; ++i){
		    	String[] colType = StringUtils.splitToArray(colTypes[i], ",", false);
		    	this.addColType(colType[0], Integer.parseInt(colType[1]));
		    }
		    
		    // Read the primary keys
		    this.primaryKeys.clear();
		    String[] pkeyStrs = StringUtils.splitToArray(scanner.nextLine(), "=", true);
		    if (pkeyStrs.length>1){
			    List<String> pkeys = StringUtils.split(pkeyStrs[1], ",", false);
			    for (int i=0; i<pkeys.size(); ++i){
			    	this.primaryKeys.add(pkeys.get(i));
			    }
		    }
		    
		    //Recover other fields.
		    this.jdbcDriver = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		    this.jdbcUrl = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		    this.dbUsername = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		    this.dbPassword = StringUtils.splitToArray(scanner.nextLine(), "=", false)[1];
		    this.indexCol = Integer.parseInt(StringUtils.splitToArray(scanner.nextLine(), "=", false)[1]);
		} catch (Exception e) {
			throw new RiteException(e);
		} finally {
			try {
				scanner.close();
			} catch (Exception e) {
			}
		}
	}

}
