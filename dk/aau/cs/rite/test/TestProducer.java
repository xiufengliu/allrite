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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import dk.aau.cs.rite.ConnectionType;
import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.producer.ProducerConnection;
import dk.aau.cs.rite.producer.flush.LazyFlush;

public class TestProducer {

	Logger log = Logger.getLogger(TestProducer.class.getName());
	
	
	//private Connection conn;
	private PreparedStatement ps;
	private volatile boolean commit = false;

	String jdbcDriver = "org.postgresql.Driver";
	String jdbcUrl = "jdbc:postgresql://localhost/xiliu";
	String dbUsername = "xiliu";
	String dbPassword = "abcd1234";
	String server = "localhost";
	int port = 5433;

	public static void main(String[] args) {

		//String inFile = "/home/xiliu/Dropbox/rite/test/csv/lineitem.csv";
		String inFile = "/tmp/dbgen/lineitem.tbl";
		int rowsBeforeCommit = 200000;
		int commitInterval = 20 * 1000;
		int sleepFactor = 0;

		BufferedReader input = null;
		try {
			input = new BufferedReader(new FileReader(inFile), 16384);
			TestProducer tester = new TestProducer();
			long[] ms = tester.start(input, commitInterval, sleepFactor, rowsBeforeCommit);
			System.out.printf("Used %d ms (of which %d ms were used for the final commit) in %s mode\n", ms[0], ms[1], "lazyflush");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				input.close();
			} catch (Exception e) {

			}
		}
	}

	public long[] start(BufferedReader input, final int commitInterval, int sleepFactor, int rowsBeforeCommit) {
		//Connection conn = null;
		ProducerConnection conn  = null;
		
		try {
			conn = (ProducerConnection) dk.aau.cs.rite.RiteDriverManager.getConnection(ConnectionType.PRODUCER, jdbcDriver, jdbcUrl, dbUsername, dbPassword, server, port);
			//conn.setFlusher(new LazyFlush());
			
			//conn = this.getJDBCConnection();
			//ps = conn.prepareStatement("INSERT INTO lineitem(datekey, partkey, suppkey, custkey, orderkey, quantity) VALUES (?, ?, ?, ?, ?, ?)");
			
			ps = conn.prepareStatement("INSERT INTO lineitem(orderkey,partkey,suppkey,linenumber,quantity,extendedprice,discount,tax,returnflag,linestatus,shipdate,commitdate,receiptdate,shipinstruct,shipmode,comment) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			PreparedStatement delPstmt = conn.prepareStatement("delete from lineitem where linenumber=?");
			//PreparedStatement delPstmt = conn.prepareStatement("update lineitem set partkey=?, suppkey=?  where quantity>=?");
			delPstmt.setInt(1, 1);
			//delPstmt.setInt(2, 9999);
			//delPstmt.setInt(3, 2);
			//delPstmt.setInt(2, 11);
			//delPstmt.setInt(3, 111);
			//delPstmt.setInt(4, 10);
			
			
			Thread ticker = null;
			if (commitInterval > 0) {
				ticker = new Thread(new Runnable() {
					public void run() {
						try {
							while (true) {
								while (commit) {
									// System.err.println("waiting for commit");
									Thread.sleep(250); // Sleep while we have a pending commit
								}
								// Now wait for commitInterval before we ask for the next commit
								Thread.sleep(commitInterval);
								commit = true;
							}
						} catch (InterruptedException e) {
							// just stop
						}
					}
				});
				ticker.setDaemon(true);
				ticker.start();
			}

			long start = System.currentTimeMillis();
			int lineNr = 0;
			String line;
			while ((line = input.readLine()) != null) {
				lineNr++;
				if (rowsBeforeCommit > 0 && lineNr % rowsBeforeCommit == 0)
					commit = true;
				if (sleepFactor > 0 && lineNr % 100 == 0) {
					try {
						Thread.sleep((long) (Math.random() * sleepFactor));
					} catch (InterruptedException e) {
						// ...
					}
				}
				
				
				List<String> items = StringUtils.split(line, "|", true);
				/*String[] items = line.split("|");
				for (int j = 0; j < 16; j++) {
					ps.setInt(i+1 , Integer.parseInt(items[j]));
				
				}*/
				
				int i = 0;
				ps.setInt(++i, Integer.parseInt(items.get(i-1)));
				ps.setInt(++i, Integer.parseInt(items.get(i-1)));
				ps.setInt(++i, Integer.parseInt(items.get(i-1)));
				ps.setInt(++i, Integer.parseInt(items.get(i-1)));
				ps.setDouble(++i, Double.parseDouble(items.get(i-1)));
				ps.setDouble(++i, Double.parseDouble(items.get(i-1)));
				ps.setDouble(++i, Double.parseDouble(items.get(i-1)));
				ps.setDouble(++i, Double.parseDouble(items.get(i-1)));
				ps.setString(++i, items.get(i-1));
				ps.setString(++i, items.get(i-1));
				ps.setDate(++i, Date.valueOf(items.get(i-1)));
				ps.setDate(++i, Date.valueOf(items.get(i-1)));
				ps.setDate(++i, Date.valueOf(items.get(i-1)));
				ps.setString(++i, items.get(i-1));
				ps.setString(++i, items.get(i-1));
				ps.setString(++i, items.get(i-1));
				
				ps.execute();
				if (commit) {
					log.info("lineNr="+ lineNr);
					conn.commit(true);
					commit = false;
				}
			} /* while */
			
			delPstmt.execute();
			System.out.println("lineNr="+ lineNr);
			long middle = System.currentTimeMillis();
			conn.commit(true); // materialize?
			long end = System.currentTimeMillis();
			ps.close();
			
			
			
			return new long[] { end - start, end - middle};
		} catch (Exception e) {
			e.printStackTrace();
		}  finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new long[] { 0, 0 };
	}
	
	
	public Connection getJDBCConnection() throws RiteException {
		try {
				Class.forName(this.jdbcDriver);
				Connection	conn = DriverManager.getConnection(this.jdbcUrl, this.dbUsername, this.dbPassword);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RiteException(e.getMessage());
		}
	}

	public static void printUsage() {
		System.err.println("USAGE:");
		System.err
				.println("java Loader INPUTFILE [commit interval (ms)] [max sleep time (ms)] [rows to insert before a commit] [mat | nomat]");
		System.err
				.println("Defaults are: <no default>   <no default>   0   0   0   nomat");
	}
}
