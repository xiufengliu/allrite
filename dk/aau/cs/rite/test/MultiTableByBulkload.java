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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.producer.ProducerConnection;

public class MultiTableByBulkload implements Test{

	private volatile boolean commit = false;
	 
	int commitInterval = 10 * 1000;
	int commitSize;
	Config config;
	
	AtomicInteger atomCounter = new AtomicInteger();
	
	public MultiTableByBulkload(Config config){
		try {
			
			this.config = config;
			
			this.commitSize = config.getCommitSize();
			this.atomCounter.set(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void insert(int size) {
		ExecutorService pool =  Executors.newFixedThreadPool(5);
		 try {
			pool.execute(new Loader(0, size));
			pool.execute(new Loader(1, size));
			pool.execute(new Loader(2, size));
			pool.execute(new Loader(3, size));
			pool.execute(new Loader(4, size));
			pool.shutdown();
			pool.awaitTermination(30, TimeUnit.MINUTES);
		 } catch (Exception ex) {
		        pool.shutdownNow();
		}  
	}

	@Override
	public void updel(int size) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void select() {
		// TODO Auto-generated method stub
		
	}

	
	protected void insertLineItemParallel(int size) throws Exception {
		Connection conn = config.getPostgreSQLConnection();
		BufferedReader bufReader = config.getInsert(size);
		String delim = "|";
		String nullStr = "";

		String line = null;
		File file = File.createTempFile("lineitem", ".csv");
		String inFile = file.getAbsolutePath();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		
		StringBuilder sqlBuilder = new StringBuilder("COPY ")
		.append("lineitem").append(" FROM '").append(inFile)
		.append("' ").append("DELIMITER '").append(delim)
		.append("' NULL '").append(nullStr).append("' CSV");
		PreparedStatement pstmt = conn.prepareStatement(sqlBuilder.toString());
		
		 StringBuilder newLine = new StringBuilder();
		 int cnt =0;
		 while ((line = bufReader.readLine()) != null) {
			 String[] vals = StringUtils.splitToArray(line, "|", false);
			 newLine.setLength(0);
			 for (int i=0; i<vals.length; ++i){
				 newLine.append(vals[i]);
				 if (i!=vals.length-1) newLine.append("|");
			 }
			 bufferedWriter.write(newLine.toString());
			 bufferedWriter.newLine();
			 
			 if (++cnt%commitSize==0){
				 bufferedWriter.flush();
				 bufferedWriter.close();
				 pstmt.execute();
				 file.delete();
				 bufferedWriter = new BufferedWriter(new FileWriter(file));
			 }
		 }
		 
		 bufferedWriter.flush();
		 bufferedWriter.close();
		 pstmt.execute();
		 pstmt.close();
		 file.delete();
	}
	
	protected void insertOrdersParallel() throws Exception {
		Connection jdbcConn = config.getPostgreSQLConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/orders1000000.csv"), 16384);
		
		
		String delim = "|";
		String nullStr = "";

		
		int commitSize = config.getCommitSize();
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
			 if (++cnt%commitSize==0){
				 out.close();
				 pstmt.execute();
				 file.delete();
				 out = new PrintWriter(new FileWriter(file));
			 }
		 }
		 out.close();
		 pstmt.execute();
		 file.delete();
	}
	
	
	
	protected void insertSupplierParallel() throws Exception { 
		Connection conn = config.getPostgreSQLConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/suppliers.csv"), 16384);
		String delim = "|";
		String nullStr = "";

		String line = null;
		File file = File.createTempFile("suppliers", ".csv");
		String inFile = file.getAbsolutePath();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		
		 StringBuilder newLine = new StringBuilder();
		 while ((line = bufReader.readLine()) != null) {
			 String[] vals = StringUtils.splitToArray(line, "|", false);
			 newLine.setLength(0);
			 for (int i=0; i<vals.length; ++i){
				 newLine.append(vals[i]);
				 if (i!=vals.length-1) newLine.append("|");
			 }
			 
			 bufferedWriter.write(newLine.toString());
			 bufferedWriter.newLine();
		 }
		 bufferedWriter.flush();
		 bufferedWriter.close();
		 

		StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("suppliers").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
		PreparedStatement pstmt = conn.prepareStatement(sqlBuilder.toString());
		pstmt.execute();
		pstmt.close();
		file.delete();
	}

	protected void insertDateParallel() throws Exception {
		Connection conn = config.getPostgreSQLConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/dates.csv"), 16384);
		String delim = "|";
		String nullStr = "";

		String line = null;
		File file = File.createTempFile("dates", ".csv");
		String inFile = file.getAbsolutePath();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		
		 StringBuilder newLine = new StringBuilder();
		 while ((line = bufReader.readLine()) != null) {
			 String[] vals = StringUtils.splitToArray(line, "|", false);
			 newLine.setLength(0);
			 for (int i=0; i<vals.length; ++i){
				 newLine.append(vals[i]);
				 if (i!=vals.length-1) newLine.append("|");
			 }
			 
			 bufferedWriter.write(newLine.toString());
			 bufferedWriter.newLine();
		 }
		 bufferedWriter.flush();
		 bufferedWriter.close();
		 

		StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("dates").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
		PreparedStatement pstmt = conn.prepareStatement(sqlBuilder.toString());
		pstmt.execute();
		pstmt.close();
		file.delete();
	}

	protected void insertCustomerParallel() throws Exception {
		Connection conn = config.getPostgreSQLConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/customers.csv"), 16384);
		String delim = "|";
		String nullStr = "";

		String line = null;
		File file = File.createTempFile("customers", ".csv");
		String inFile = file.getAbsolutePath();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
		
		 StringBuilder newLine = new StringBuilder();
		 while ((line = bufReader.readLine()) != null) {
			 String[] vals = StringUtils.splitToArray(line, "|", false);
			 newLine.setLength(0);
			 for (int i=0; i<vals.length; ++i){
				 newLine.append(vals[i]);
				 if (i!=vals.length-1) newLine.append("|");
			 }
			 
			 bufferedWriter.write(newLine.toString());
			 bufferedWriter.newLine();
		 }
		 bufferedWriter.flush();
		 bufferedWriter.close();
		 

		StringBuilder sqlBuilder = new StringBuilder("COPY ")
				.append("customers").append(" FROM '").append(inFile)
				.append("' ").append("DELIMITER '").append(delim)
				.append("' NULL '").append(nullStr).append("' CSV");
		PreparedStatement pstmt = conn.prepareStatement(sqlBuilder.toString());
		pstmt.execute();
		pstmt.close();
		file.delete();
	}

	class Loader implements Runnable{
		int i;
		int size;
		public Loader(int i, int size){
			this.i = i;
			this.size = size;
		}	
		@Override
		public void run(){
			try {
			switch (this.i){
				case 0: insertLineItemParallel(size); break;
				case 1: insertCustomerParallel(); break;
				case 2: insertOrdersParallel(); break;
				case 3: insertDateParallel(); break;
				case 4: insertSupplierParallel(); break;
			}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void update(int size) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(int size) {
		// TODO Auto-generated method stub
		
	}

	
}
