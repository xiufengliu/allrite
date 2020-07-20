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
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.producer.ProducerConnection;
import dk.aau.cs.rite.producer.flush.LazyFlush;

public class RiTEJDBC implements Test{

	private boolean materialized = false;
	
	boolean lazyFlush = false; 
	int commitInterval = 10 * 1000;
	int commitSize;
	Config config;
	
	AtomicInteger atomCounter = new AtomicInteger();
	
	public RiTEJDBC(Config config,  boolean lazyFlush, boolean materialized){
		
		this.config = config;
		this.lazyFlush = lazyFlush;
		this.materialized = materialized;
		this.commitSize = config.getCommitSize();
		this.atomCounter.set(0);
	}
	
	@Override
	public void insert(int size) {
		try {
			String tableName = config.getTableName();
			if ("lineitem".equals(tableName)) {
				insertLineItem(size);
			} else if ("orders".equals(tableName)) {
				insertOrders(size);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	protected void insertOrders(int size) throws Exception {
		ProducerConnection conn  = null;
		BufferedReader bufReader = config.getInsert(size);
		conn = config.getRiTEProducerConnection();
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush(config.getDataFreshness()));
		} 
		PreparedStatement ps = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");	
		int cnt = 0;
		String line;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			int i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setString(++i, vals[i - 1]);
			ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
			ps.setDate(++i, Date.valueOf(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			if ((++cnt)%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		
		conn.commit(true);
	}
	
	
	protected void insertLineItem(int size) throws Exception {
		ProducerConnection conn  = null;
		BufferedReader bufReader = config.getInsert(size);
		conn = config.getRiTEProducerConnection();
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		String sql = "INSERT INTO lineitem(orderkey, custkey, suppkey, datekey, quantity) VALUES (?, ?, ?, ?, ?)";
		PreparedStatement ps = conn.prepareStatement(sql);	
		
		String line;
		
		int i, cnt = 0;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			if ((++cnt)%commitSize==0){
				conn.commit(false);
			}
		}
		ps.close();
		conn.commit(this.materialized);
	}
	
	
//	@Override
	public void updel_bak(int size) {
		try {
			BufferedReader insertReader = config.getInsert(size);		
			ProducerConnection conn = config.getRiTEProducerConnection();
			
			PreparedStatement insertPstmt = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");
			PreparedStatement updatePstmt = conn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
			PreparedStatement deletePstmt = conn.prepareStatement("delete from orders where orderkey=?");
			
			int i, cnt = 0;
			String line;
			while ((line = insertReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				int type = Integer.parseInt(vals[0]);
				i = 0;
				if (type==1){
					insertPstmt.setInt(++i, Integer.parseInt(vals[i]));
					insertPstmt.setString(++i, vals[i]);
					insertPstmt.setDouble(++i, Double.parseDouble(vals[i]));
					insertPstmt.setDate(++i, Date.valueOf(vals[i]));
		
					insertPstmt.setInt(++i, Integer.parseInt(vals[i]));
					insertPstmt.execute();
				} else if (type==2){
					updatePstmt.setString(++i, vals[i+1]);
					updatePstmt.setDouble(++i, Double.parseDouble(vals[i+1]));
					updatePstmt.setDate(++i, Date.valueOf(vals[i+1]));
					updatePstmt.setInt(++i, Integer.parseInt(vals[i+1]));
					
					updatePstmt.setInt(++i, Integer.parseInt(vals[1])); // where clause
					updatePstmt.execute();
				} else if (type==3){
					deletePstmt.setInt(1, Integer.parseInt(vals[1])); // where clause
					deletePstmt.execute();
				}
				
				if ((++cnt)%commitSize==0){
					conn.commit();
				}
			}
			insertPstmt.close();
			updatePstmt.close();
			deletePstmt.close();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	public void updel(int size)  {
		try{
		ProducerConnection conn  = null;
		BufferedReader insertReader = config.getInsert(10);
		BufferedReader updateReader = config.getUpdate(size);
		BufferedReader deleteReader = config.getDelete(size);
		
		conn = config.getRiTEProducerConnection();
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush(config.getDataFreshness()));
		}
		
		PreparedStatement insertPstmt = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");
		PreparedStatement updatePstmt = conn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
		PreparedStatement deletePstmt = conn.prepareStatement("delete from orders where orderkey=?");
		
		int i, lineNr = 0;
		String line;
		while ((line = insertReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			insertPstmt.setInt(++i, Integer.parseInt(vals[i - 1]));
			insertPstmt.setString(++i, vals[i - 1]);
			insertPstmt.setDouble(++i, Double.parseDouble(vals[i - 1]));
			insertPstmt.setDate(++i, Date.valueOf(vals[i - 1]));
			insertPstmt.setInt(++i, Integer.parseInt(vals[i - 1]));
			insertPstmt.execute();
			
			if ((lineNr++)%commitSize==0){
				conn.commit();
			}
		}
		insertPstmt.close();
		conn.commit();
		System.out.println("Finish inserts");

		lineNr = 0;
		while ((line = updateReader.readLine()) != null) { // update
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			updatePstmt.setString(++i, vals[i]);
			updatePstmt.setDouble(++i, Double.parseDouble(vals[i]));
			updatePstmt.setDate(++i, Date.valueOf(vals[i]));
			updatePstmt.setInt(++i, Integer.parseInt(vals[i]));
			
			updatePstmt.setInt(++i, Integer.parseInt(vals[0])); // where clause
			updatePstmt.execute();
			if ((lineNr++)%commitSize==0){
				conn.commit();
			}
		}
		updatePstmt.close();
		System.out.println("Finish updates");
		lineNr = 0;
		while ((line = deleteReader.readLine()) != null) {// delete
			deletePstmt.setInt(1, Integer.parseInt(line)); // where clause
			deletePstmt.execute();
			if ((lineNr++)%commitSize==0){
				conn.commit();
			}
		}
		deletePstmt.close();
		conn.commit();
		System.out.println("Finish deletes");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	


	@Override
	public void select() {
		try {
			BufferedReader insertReader = config.getInsert(1);		
			ProducerConnection conn = config.getRiTEProducerConnection();
			
			PreparedStatement insertPstmt = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");
			PreparedStatement updatePstmt = conn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
			PreparedStatement deletePstmt = conn.prepareStatement("delete from orders where orderkey=?");
			PreparedStatement selectPstmt = conn.prepareStatement("select orderkey, orderstatus, totalprice, orderdate, shippriority from orders");
			
			
			int i, cnt = 0;
			String line;
			while ((line = insertReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				int type = Integer.parseInt(vals[0]);
				i = 0;
				if (type==1){
					insertPstmt.setInt(++i, Integer.parseInt(vals[i]));
					insertPstmt.setString(++i, vals[i]);
					insertPstmt.setDouble(++i, Double.parseDouble(vals[i]));
					insertPstmt.setDate(++i, Date.valueOf(vals[i]));
		
					insertPstmt.setInt(++i, Integer.parseInt(vals[i]));
					insertPstmt.execute();
				} else if (type==2){
					updatePstmt.setString(++i, vals[i+1]);
					updatePstmt.setDouble(++i, Double.parseDouble(vals[i+1]));
					updatePstmt.setDate(++i, Date.valueOf(vals[i+1]));
					updatePstmt.setInt(++i, Integer.parseInt(vals[i+1]));
					
					updatePstmt.setInt(++i, Integer.parseInt(vals[1])); // where clause
					updatePstmt.execute();
				} else if (type==3){
					deletePstmt.setInt(1, Integer.parseInt(vals[1])); // where clause
					deletePstmt.execute();
				}
				
				if ((++cnt)%commitSize==0){
					conn.commit();
				}
			}
			insertPstmt.close();
			updatePstmt.close();
			deletePstmt.close();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void update(int size) {
		ProducerConnection conn  = null;
		BufferedReader bufReader;
		try {
			bufReader = config.getInsert(size);
		
		conn = config.getRiTEProducerConnection();
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		PreparedStatement ps = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");	
		int cnt = 0;
		String line;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			int i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setString(++i, vals[i - 1]);
			ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
			ps.setDate(++i, Date.valueOf(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			
			if ((++cnt)%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		
		PreparedStatement updatePstmt = conn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey<?");
		int i = 0;
		updatePstmt.setString(++i, "Y");
		updatePstmt.setDouble(++i, 999.99);
		updatePstmt.setDate(++i, Date.valueOf("2099-09-09"));
		updatePstmt.setInt(++i, 99);
		
		updatePstmt.setInt(++i, config.getRangeValue()); // where clause
		updatePstmt.execute();
		updatePstmt.close();
		conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void delete(int size) {
		ProducerConnection conn  = null;
		BufferedReader bufReader;
		try {
			bufReader = config.getInsert(size);
		
		conn = config.getRiTEProducerConnection();
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");	
		int cnt = 0;
		String line;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			int i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setString(++i, vals[i - 1]);
			ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
			ps.setDate(++i, Date.valueOf(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			
			if ((++cnt)%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		
		PreparedStatement updatePstmt = conn.prepareStatement("delete from orders where orderkey<?");
		updatePstmt.setInt(1, config.getRangeValue()); // where clause
		updatePstmt.execute();
		updatePstmt.close();
		conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
