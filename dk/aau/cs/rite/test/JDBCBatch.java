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
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

import dk.aau.cs.rite.common.StringUtils;

public class JDBCBatch implements Test {

	Config config;
	int commitSize;
	public JDBCBatch(Config config) {
		this.config = config;
		
	}

	@Override
	public void insert(int size) {
		this.commitSize = config.getCommitSize();
		String tableName = config.getTableName();
		if ("lineitem".equals(tableName)){
			insertLineItem(size);
		} else	if ("orders".equals(tableName)){
			insertOrders(size);
		}
	}
	
	
	public void insertOrders(int size) {
		Connection jdbcConn = null;
		BufferedReader bufReader = null;
		try {
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			bufReader = config.getInsert(size);
			String sql = "INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)";
			PreparedStatement ps = jdbcConn.prepareStatement(sql);
			String line;
			int cnt = 0;
			int i = 0;
			while ((line = bufReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				i = 0;
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setString(++i, vals[i - 1]);
				ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
				ps.setDate(++i, Date.valueOf(vals[i - 1]));

				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.addBatch();
				if (++cnt % 500 == 0) {
					int[] updateCounts = ps.executeBatch();
				}
				if (cnt % commitSize == 0) {
				//	int[] updateCounts = ps.executeBatch();
					jdbcConn.commit();
				}
			}
			int[] updateCounts = ps.executeBatch();
			ps.close();
			jdbcConn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void insertLineItem(int size) {
		Connection jdbcConn = null;
		BufferedReader bufReader = null;
		try {
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			bufReader = config.getInsert(size);
			String sql = "INSERT INTO lineitem(orderkey,custkey, suppkey,datekey, quantity) VALUES (?,  ?, ?, ?, ?)";
			PreparedStatement ps = jdbcConn.prepareStatement(sql);
			String line;
			int cnt = 0;
			int i = 0;
			while ((line = bufReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				i = 0;
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.addBatch();
				if (++cnt % 500 == 0) {
					int[] updateCounts = ps.executeBatch();
				}
				if (cnt % commitSize == 0) {
					//int[] updateCounts = ps.executeBatch();
					jdbcConn.commit();
				}
			}
			int[] updateCounts = ps.executeBatch();
			ps.close();
			jdbcConn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//@Override
	public void updel_bak(int size) {
		try {
			BufferedReader insertReader = config.getInsert(size);		
			Connection conn = config.getPostgreSQLConnection();
			conn.setAutoCommit(false);

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
					insertPstmt.addBatch();
				} else if (type==2){
					updatePstmt.setString(++i, vals[i+1]);
					updatePstmt.setDouble(++i, Double.parseDouble(vals[i+1]));
					updatePstmt.setDate(++i, Date.valueOf(vals[i+1]));
					updatePstmt.setInt(++i, Integer.parseInt(vals[i+1]));
					
					updatePstmt.setInt(++i, Integer.parseInt(vals[1])); // where clause
					updatePstmt.addBatch();
				} else if (type==3){
					deletePstmt.setInt(1, Integer.parseInt(vals[1])); // where clause
					deletePstmt.addBatch();
				}
				++cnt;
				
				if (cnt%100==0){
					insertPstmt.executeBatch();
					updatePstmt.executeBatch();
					deletePstmt.executeBatch();
				}
				
				if (cnt%commitSize==0){
					conn.commit();
				}
			}
			insertPstmt.executeBatch();
			updatePstmt.executeBatch();
			deletePstmt.executeBatch();
			insertPstmt.close();
			updatePstmt.close();
			deletePstmt.close();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	
	
	public void updel(int size) {
		try {
			Connection conn = null;
			BufferedReader insertReader = config.getInsert(size);
			BufferedReader updateReader = config.getUpdate(size);
			BufferedReader deleteReader = config.getDelete(size);

			conn = config.getPostgreSQLConnection();
			conn.setAutoCommit(false);

			PreparedStatement insertPstmt = conn
					.prepareStatement("INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");
			PreparedStatement updatePstmt = conn
					.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
			PreparedStatement deletePstmt = conn
					.prepareStatement("delete from orders where orderkey=?");

			int cnt = 0;
			String line;
			System.out.println("start load");
			while ((line = insertReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				int i = 0;
				insertPstmt.setInt(++i, Integer.parseInt(vals[i - 1]));
				insertPstmt.setString(++i, vals[i - 1]);
				insertPstmt.setDouble(++i, Double.parseDouble(vals[i - 1]));
				insertPstmt.setDate(++i, Date.valueOf(vals[i - 1]));
				
				insertPstmt.setInt(++i, Integer.parseInt(vals[i - 1]));
				
				insertPstmt.addBatch();
				if (++cnt % 1000 == 0) {
					int[] insertCounts = insertPstmt.executeBatch();
				}			
				if (cnt % commitSize == 0) {
					conn.commit();
				}
			}
			int[] updateCounts = insertPstmt.executeBatch();
			insertPstmt.close();
			conn.commit();
			System.out.println("Finished load");
			cnt = 0;
			while ((line = updateReader.readLine()) != null) { // update
				String[] vals = StringUtils.splitToArray(line, "|", false);
				int i = 0;
				updatePstmt.setString(++i, vals[i]);
				updatePstmt.setDouble(++i, Double.parseDouble(vals[i]));
				updatePstmt.setDate(++i, Date.valueOf(vals[i]));
				updatePstmt.setInt(++i, Integer.parseInt(vals[i]));

				updatePstmt.setInt(++i, Integer.parseInt(vals[0])); // where clause
				
				updatePstmt.addBatch();
				if (++cnt % 100 == 0) {
					updatePstmt.executeBatch();
					conn.commit();
				}
			}
			
			updatePstmt.executeBatch();
			updatePstmt.close();
			conn.commit();
			System.out.println("Finished update");
			cnt = 0;
			while ((line = deleteReader.readLine()) != null) {// delete
				deletePstmt.setInt(1, Integer.parseInt(line)); // where clause
				deletePstmt.execute();
				deletePstmt.addBatch();
				if (++cnt % 100 == 0) {
					deletePstmt.executeBatch();
					conn.commit();
				}
			}
			deletePstmt.executeBatch();
			deletePstmt.close();
			conn.commit();
			System.out.println("Finished delete");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void select() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(int size) {
		Connection jdbcConn = null;
		BufferedReader bufReader = null;
		try {
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			bufReader = config.getInsert(size);
			String sql = "INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)";
			PreparedStatement ps = jdbcConn.prepareStatement(sql);
			String line;
			int cnt = 0;
			int i = 0;
			while ((line = bufReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				i = 0;
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setString(++i, vals[i - 1]);
				ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
				ps.setDate(++i, Date.valueOf(vals[i - 1]));

				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.addBatch();
				if (++cnt % 2000 == 0) {
					int[] updateCounts = ps.executeBatch();
				}
				if (cnt % commitSize == 0) {
					jdbcConn.commit();
				}
			}
			int[] updateCounts = ps.executeBatch();
			ps.close();
			jdbcConn.commit();
			
			
			PreparedStatement updatePstmt = jdbcConn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey<?");
			i = 0;
			updatePstmt.setString(++i, "Y");
			updatePstmt.setDouble(++i, 999.99);
			updatePstmt.setDate(++i, Date.valueOf("2099-09-09"));
			updatePstmt.setInt(++i, 99);
			
			updatePstmt.setInt(++i, config.getRangeValue()); // where clause
			updatePstmt.execute();
			updatePstmt.close();
			jdbcConn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void delete(int size) {
		Connection jdbcConn = null;
		BufferedReader bufReader = null;
		try {
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			bufReader = config.getInsert(size);
			String sql = "INSERT INTO orders(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)";
			PreparedStatement ps = jdbcConn.prepareStatement(sql);
			String line;
			int cnt = 0;
			int i = 0;
			while ((line = bufReader.readLine()) != null) {
				String[] vals = StringUtils.splitToArray(line, "|", false);
				i = 0;
				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.setString(++i, vals[i - 1]);
				ps.setDouble(++i, Double.parseDouble(vals[i - 1]));
				ps.setDate(++i, Date.valueOf(vals[i - 1]));

				ps.setInt(++i, Integer.parseInt(vals[i - 1]));
				ps.addBatch();
				if (++cnt % 2000 == 0) {
					int[] updateCounts = ps.executeBatch();
				}
				if (cnt % commitSize == 0) {
					jdbcConn.commit();
				}
			}
			int[] updateCounts = ps.executeBatch();
			ps.close();
			jdbcConn.commit();
			
			PreparedStatement deletePstmt = jdbcConn.prepareStatement("delete from orders where orderkey<?");
			deletePstmt.setInt(1, config.getRangeValue()); // where clause
			deletePstmt.execute();
			deletePstmt.close();
			jdbcConn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
