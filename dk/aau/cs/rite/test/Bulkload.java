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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

import dk.aau.cs.rite.common.StringUtils;

public class Bulkload implements Test {
	Config config;

	int commitSize;
	public Bulkload(Config config){
		this.config = config;
	
	}
	
	@Override
	public void insert(int size) {
		try {
			this.commitSize = config.getCommitSize();
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
	
	
	public void insertOrders(int size) {
		
		Connection jdbcConn = null;
		try{
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			String delim = "|";
			String nullStr = "";
			 BufferedReader bufReader = config.getInsert(size);
			 String line = null;
			 
			 File file = File.createTempFile("orders", ".csv");
			 String inFile = file.getAbsolutePath();
			 PrintWriter  out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			 int cnt = 0;
			 StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("orders").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
			 PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			 
			 while ((line = bufReader.readLine()) != null) {
				 String[] vals = StringUtils.splitToArray(line, "|", false);
				 for (int i=0; i<vals.length; ++i){
					 out.print(vals[i]);
					 if (i!=vals.length-1)  out.print("|");
				 }
				 out.println();
				 if ((++cnt)%commitSize==0){
					 out.close();
					 pstmt.execute();
					// System.out.println(cnt);
					 jdbcConn.commit();
					 file.delete();
					 out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
				 }
			 }
			 out.close();
			 pstmt.execute();
			 jdbcConn.commit();
			 file.delete();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}


	
	
	public void insertLineItem(int size) {
		
		Connection jdbcConn = null;
		try{
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			String delim = "|";
			String nullStr = "";
			 BufferedReader bufReader = config.getInsert(size);
			 String line = null;
			 
			 File file = File.createTempFile("lineitem", ".csv");
			 String inFile = file.getAbsolutePath();
			 PrintWriter  out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			 int cnt = 0;
			 StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("lineitem").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
			 PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			 
			 while ((line = bufReader.readLine()) != null) {
				 String[] vals = StringUtils.splitToArray(line, "|", false);
				 for (int i=0; i<vals.length; ++i){
					 out.print(vals[i]);
					 if (i!=vals.length-1)  out.print("|");
				 }
				 out.println();
				 if ((++cnt)%commitSize==0){
					 out.close();
					 pstmt.execute();
					// System.out.println(cnt);
					 jdbcConn.commit();
					 file.delete();
					 out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
				 }
			 }
			 out.close();
			 pstmt.execute();
			 jdbcConn.commit();
			 file.delete();
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
			
			String delim = "|";
			String nullStr = "";
			File file = File.createTempFile("orders", ".csv");
			String inFile = file.getAbsolutePath();
			PrintWriter  out = new PrintWriter(new FileWriter(file));
			StringBuilder sqlBuilder = new StringBuilder("COPY ")
			.append("orders").append(" FROM '").append(inFile)
			.append("' ").append("DELIMITER '").append(delim)
			.append("' NULL '").append(nullStr).append("' CSV");
			
			PreparedStatement insertPstmt = conn.prepareStatement(sqlBuilder.toString());
			PreparedStatement updatePstmt = conn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
			PreparedStatement deletePstmt = conn.prepareStatement("delete from orders where orderkey=?");
			
			int i=0, cnt = 0;
			String line;
			while ((line = insertReader.readLine()) != null) {
				i = 0;
				String[] vals = StringUtils.splitToArray(line, "|", false);
				int type = Integer.parseInt(vals[0]);
				if (type==1){
					 for (int j=1; j<vals.length; ++j){
						 out.print(vals[j]);
						 if (j!=vals.length-1)  out.print("|");
					 }
					 out.println();
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
					 out.close();
					 insertPstmt.execute();
					 file.delete();
					 out = new PrintWriter(new FileWriter(file));
					conn.commit();
				}
			}
			out.close();
			insertPstmt.execute();
			file.delete();
			updatePstmt.close();
			deletePstmt.close();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	
	public void updel(int size) {
		Connection jdbcConn = null;
		 String line = null;
		try{
			
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(false);
			PreparedStatement updatePstmt = jdbcConn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey=?");
			PreparedStatement deletePstmt = jdbcConn.prepareStatement("delete from orders where orderkey=?");
			
			String delim = "|";
			String nullStr = "";
	
			 BufferedReader bufReader = config.getInsert(size);
			 BufferedReader updateReader = config.getUpdate(size);
			 BufferedReader deleteReader = config.getDelete(size);
			 
			
			 File file = File.createTempFile("orders", ".csv");
			 String inFile = file.getAbsolutePath();
			 PrintWriter  out = new PrintWriter(new FileWriter(file));
			 int cnt = 0;
			 StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("orders").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
			 PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			 
			 while ((line = bufReader.readLine()) != null) {
				 String[] vals = StringUtils.splitToArray(line, "|", false);
				 for (int i=0; i<vals.length; ++i){
					 out.print(vals[i]);
					 if (i!=vals.length-1)  out.print("|");
				 }
				 out.println();
				 if ((++cnt)%commitSize==0){
					 out.close();
					 pstmt.execute();
					 file.delete();
					 out = new PrintWriter(new FileWriter(file));
				 }
			 }
			 out.close();
			 pstmt.execute();
			 file.delete();
			
			System.out.println("Finished load");
			int i=0;
			 cnt = 0;
			while ((line = updateReader.readLine()) != null) { // update
				String[] vals = StringUtils.splitToArray(line, "|", false);
				i = 0;
				updatePstmt.setString(++i, vals[i]);
				updatePstmt.setDouble(++i, Double.parseDouble(vals[i]));
				updatePstmt.setDate(++i, Date.valueOf(vals[i]));
				updatePstmt.setInt(++i, Integer.parseInt(vals[i]));
				
				updatePstmt.setInt(++i, Integer.parseInt(vals[0])); // where clause
				 updatePstmt.execute();
				 
				 if ((++cnt)%commitSize==0){
					 jdbcConn.commit();
					}
				/*updatePstmt.addBatch();
				if (++cnt % commitSize == 0) {
					int[] updateCounts = updatePstmt.executeBatch();
				}*/
				
			}
			updatePstmt.close();
			System.out.println("Finished update");
			cnt = 0;
			while ((line = deleteReader.readLine()) != null) {// delete
				deletePstmt.setInt(1, Integer.parseInt(line)); // where clause
				deletePstmt.execute();
				if ((++cnt)%commitSize==0){
					jdbcConn.commit();
				}
				/*if (++cnt % commitSize == 0) {
					int[] delCounts = deletePstmt.executeBatch();
				}*/
			}
			deletePstmt.close();
			System.out.println("Finished delete");
		} catch (Exception e) {
			System.out.println(line);
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
		try{
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(true);
			
			String delim = "|";
			String nullStr = "";
	
			 BufferedReader bufReader = config.getInsert(size);
			 String line = null;
			 
			 File file = File.createTempFile("orders", ".csv");
			 String inFile = file.getAbsolutePath();
			 PrintWriter  out = new PrintWriter(new FileWriter(file));
			 int cnt = 0;
			 StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("orders").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
			 PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			 
			 while ((line = bufReader.readLine()) != null) {
				 String[] vals = StringUtils.splitToArray(line, "|", false);
				 for (int i=0; i<vals.length; ++i){
					 out.print(vals[i]);
					 if (i!=vals.length-1)  out.print("|");
				 }
				 out.println();
				 if ((++cnt)%commitSize==0){
					 out.close();
					 pstmt.execute();
					 file.delete();
					 out = new PrintWriter(new FileWriter(file));
				 }
			 }
			 out.close();
			 pstmt.execute();
			 file.delete();
			
			PreparedStatement updatePstmt = jdbcConn.prepareStatement("update orders set orderstatus=?,totalprice=?,orderdate=?,shippriority=? where orderkey<?");
			int i = 0;
			updatePstmt.setString(++i, "Y");
			updatePstmt.setDouble(++i, 999.99);
			updatePstmt.setDate(++i, Date.valueOf("2099-09-09"));
			updatePstmt.setInt(++i, 99);
			
			updatePstmt.setInt(++i, config.getRangeValue()); // where clause
			updatePstmt.execute();
			updatePstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void delete(int size) {
		Connection jdbcConn = null;
		try{
			jdbcConn = config.getPostgreSQLConnection();
			
			String delim = "|";
			String nullStr = "";
	
			 BufferedReader bufReader = config.getInsert(size);
			 String line = null;
			 File file = File.createTempFile("orders", ".csv");
			 String inFile = file.getAbsolutePath();
			 PrintWriter  out = new PrintWriter(new FileWriter(file));
			 int cnt = 0;
			 StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("orders").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
			 PreparedStatement pstmt = jdbcConn.prepareStatement(sqlBuilder.toString());
			 
			 while ((line = bufReader.readLine()) != null) {
				 String[] vals = StringUtils.splitToArray(line, "|", false);
				 for (int i=0; i<vals.length; ++i){
					 out.print(vals[i]);
					 if (i!=vals.length-1)  out.print("|");
				 }
				 out.println();
				 if ((++cnt)%commitSize==0){
					 out.close();
					 pstmt.execute();
					 file.delete();
					 out = new PrintWriter(new FileWriter(file));
				 }
			 }
			 out.close();
			 pstmt.execute();
			 file.delete();
			
			PreparedStatement deletePstmt = jdbcConn.prepareStatement("delete from orders where orderkey<?");
			deletePstmt.setInt(1, config.getRangeValue()); // where clause
			deletePstmt.execute();
			deletePstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}
