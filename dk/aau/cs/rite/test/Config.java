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
package dk.aau.cs.rite.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import dk.aau.cs.rite.ConnectionType;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.consumer.ConsumerConnection;
import dk.aau.cs.rite.producer.ProducerConnection;

public class Config {
	String DB_PASSWORD = "db.password";
	String DB_USERNAME = "db.username";
	String DB_DRIVER = "db.driver";
	String DB_NAME = "db.dbname";
	String DB_URL = "db.url";

	String CATALYST_HOST = "catalyst.host";
	String CATALYST_PORT = "catalyst.port";

	
	String TABLE_NAME = "table.name";
	
	String FILE_INSERT = "file.insert";
	String FILE_UPDATE = "file.update";
	String FILE_DELETE = "file.delete";

	String FILESIZE_START = "filesize.start";
	int fileSizeStart = -1;
	String FILESIZE_END = "filesize.end";
	int fileSizeEnd = -1;
	String RANGE_VALUE =  "range.value";
	int rangeValue = -1;
	String COMMIT_SIZE ="commit.size";
	int commitSize = -1;
	
	
	String RUN_OP = "run.op";
	String TEST_TRADJDBC ="test.tradjdbc";
	String TEST_JDBCBATCH ="test.jdbcbatch";
	String TEST_BULKLOAD ="test.bulkload";
	String TEST_INSTANTNOMAT ="test.instantnomat";
	String TEST_INSTANTWITHMAT ="test.instantwithmat";
	String TEST_LAZYNOMAT ="test.lazynomat";
	String TEST_LAZYWITHMAT ="test.lazywithmat";
	String TEST_MULTI_TABLE = "test.multitable";
	
	int freshness = -1;
	String QUERY_FRESHNESS ="query.data.freshness";
	
	
	ProducerConnection riteProdConn;
	ConsumerConnection riteConConn;
	Connection jdbcConn;
	BufferedReader insertReader, updateReader, deleteReader;
	
		
	
	Properties props;
	String configPath = "/home/xiliu/workspace/rite/test/config.xml";
	String names[] = new String[]{"Traditional JDBC", "JDBC with batches", "Bulk loading", "InstantFlush with no mat",
			"InstantFlush with mat", "LazyFlush with no mat", "LazyFlush with mat", "Multi tables"};
	
	public Config(){
		props = new Properties();
		loadConfig(configPath);
	}
	
	public void loadConfig(String filePath) {
		try {
			File configFile = new File(filePath);
			props.loadFromXML(new FileInputStream(configFile));
		} catch (Exception e) {
			System.err.printf("Cannot load the configuration file at: %s\n", filePath);
		} 
	}
	
	public String getNameByID(int id){
		if (id>=0 && id<names.length)
			return names[id];
		 else 
			return null;
	}
	
	public void reload(){
		loadConfig(configPath);
	}
	
	public String getProperty(String key) {
		return props.getProperty(key);
	}
	
	public String getString(String key){
		return props.getProperty(key);
	}
	
	public int getInt(String key){
		return Integer.parseInt(props.getProperty(key));
	}
 
	public boolean booleanValue(String key) {
		return Boolean.parseBoolean(props.getProperty(key));
	}
	
	public int getCommitSize(){
		return this.commitSize==-1?getInt(COMMIT_SIZE):this.commitSize;
	}
	
	public void setCommitSize(int commitSize){
		this.commitSize = commitSize;
	}
	
	public void setDataFreshness(int freshness){
		this.freshness = freshness;
	}
	
	public int getDataFreshness(){
		return this.freshness==-1?getInt(QUERY_FRESHNESS):this.freshness;
	}
	
	public String getTableName(){
		return props.getProperty(TABLE_NAME);
	}
	
	public void setStartSize(int startSize){
		this.fileSizeStart = startSize;
	}
	
	public int getStartSize(){
		return this.fileSizeStart==-1?Integer.parseInt(getProperty(FILESIZE_START)):this.fileSizeStart;
	}
	
	public void setEndSize(int endSize){
		this.fileSizeEnd = endSize;
	}
	
	public int getEndSize(){
		return this.fileSizeEnd==-1?Integer.parseInt(getProperty(FILESIZE_END)):this.fileSizeEnd;
	}
	
	public String getOP() {
		return this.getProperty(RUN_OP);
	}
	
	public void setRangeValue(int rangeValue){
		this.rangeValue = rangeValue;
	}
	
	public int getRangeValue() {
		return  this.rangeValue==-1?Integer.parseInt(this.getProperty(this.RANGE_VALUE)):this.rangeValue;
	}


	
	synchronized public  ProducerConnection getRiTEProducerConnection() throws Exception {
		if (this.riteProdConn==null){
			riteProdConn = newRiTEProducerConnection(Integer.parseInt(getProperty(CATALYST_PORT)));
		}
		return riteProdConn;
	}

	 public  ProducerConnection newRiTEProducerConnection(int port) throws Exception {
		 return	(ProducerConnection) dk.aau.cs.rite.RiteDriverManager
					.getConnection(ConnectionType.PRODUCER, getProperty(DB_DRIVER),
							getProperty(DB_URL), getProperty(DB_USERNAME), getProperty(DB_PASSWORD), 
							getProperty(CATALYST_HOST), port);
	}
	
	
	
	public ConsumerConnection getRiTEConsumerConnection() throws Exception {
		if (riteConConn == null) {
			riteConConn = (ConsumerConnection) dk.aau.cs.rite.RiteDriverManager
					.getConnection(ConnectionType.CONSUMER, getProperty(DB_DRIVER),
							getProperty(DB_URL), getProperty(DB_USERNAME), getProperty(DB_PASSWORD), 
							getProperty(CATALYST_HOST), Integer.parseInt(getProperty(CATALYST_PORT)));
		}
		return riteConConn;
	}

	synchronized public Connection getPostgreSQLConnection() throws Exception {
		if (jdbcConn == null) {
			Class.forName(getProperty(DB_DRIVER));
			jdbcConn = DriverManager.getConnection(getProperty(DB_URL), getProperty(DB_USERNAME), getProperty(DB_PASSWORD));
		}
		return jdbcConn;
	}
	
	public Connection newJDBCConnection() throws Exception {
			Class.forName(getProperty(DB_DRIVER));
			return DriverManager.getConnection(getProperty(DB_URL), getProperty(DB_USERNAME), getProperty(DB_PASSWORD));
	}

	public BufferedReader getInsert(int size) throws FileNotFoundException {
			this.insertReader = newDataSource(size);
			return this.insertReader;
	}

	public BufferedReader newDataSource(int size) throws FileNotFoundException {
		String fileName = String.format(getProperty(FILE_INSERT), size);
		System.out.println("filename="+fileName);
		return new BufferedReader(new FileReader(fileName), 8192*2);
	}
	
	
	public BufferedReader getUpdate(int size) throws FileNotFoundException {
			String fileName = String.format(getProperty(FILE_UPDATE), size);
			System.out.println(fileName);
			updateReader = new BufferedReader(new FileReader(fileName));
			return this.updateReader;
	}

	public BufferedReader getDelete(int size) throws FileNotFoundException {
		String fileName = String.format(getProperty(FILE_DELETE), size);
		System.out.println(fileName);
		deleteReader = new BufferedReader(new FileReader(fileName));
		return this.deleteReader;
	}



	synchronized public void close() {
		try {
		
		riteProdConn.commit(booleanValue(TEST_LAZYWITHMAT)||booleanValue(TEST_INSTANTWITHMAT));
		riteProdConn.close();
		} catch (Exception e) {
		}
		try {
			riteConConn.close();

		} catch (Exception e) {
		}
		try {
			jdbcConn.close();

		} catch (Exception e) {
		}
		try {
			insertReader.close();
		} catch (Exception e) {
		}
		try {
			updateReader.close();
		} catch (Exception e) {
		}
		try {
			deleteReader.close();

		} catch (Exception e) {
		}
		riteProdConn = null;
		jdbcConn = null;
		riteConConn = null;
		insertReader = null;
		updateReader = null;
		deleteReader = null;
	}

	public void startCatalystServer() {
		try {
			List<String> cmds = new ArrayList<String>();
			cmds.add("/usr/bin/killall");
			cmds.add("-9");
			cmds.add("/home/xiliu/workspace/rite/build/dist/server.jar");
			Utils.execShellCmd(cmds, "/tmp", false);
			cmds.clear();

			cmds.add("/usr/bin/java");
			cmds.add("-jar");
			cmds.add("/home/xiliu/workspace/rite/build/dist/server.jar");
			Utils.execShellCmd(cmds, "/tmp", false);
			Thread.sleep(10 * 1000);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
