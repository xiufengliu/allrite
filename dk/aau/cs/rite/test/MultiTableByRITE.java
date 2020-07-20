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
import java.io.FileReader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.producer.ProducerConnection;
import dk.aau.cs.rite.producer.flush.LazyFlush;

public class MultiTableByRITE implements Test{

	private volatile boolean commit = false;
	
	boolean lazyFlush = false; 
	int commitInterval = 10 * 1000;
	int commitSize;
	Config config;
	
	AtomicInteger atomCounter = new AtomicInteger();
	
	
	public MultiTableByRITE(Config config,  boolean lazyFlush){
		try {
			this.config = config;
			this.lazyFlush = lazyFlush;
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
			//pool.execute(new Loader(1, size));
			//pool.execute(new Loader(2, size));
			//pool.execute(new Loader(3, size));
			//pool.execute(new Loader(4, size));
			//pool.execute(new Loader(5, size));
			//pool.execute(new Loader(6, size));
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
		ProducerConnection conn = config.newRiTEProducerConnection(5433);
		//BufferedReader bufReader = config.getInsert(size); 
		//ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader =  config.newDataSource(size);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
			System.out.printf("Lazy flush, Commit Size=%d\n", commitSize);
		} else {
			System.out.printf("Instant flush, Commit Size=%d\n", commitSize);
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO lineitem(orderkey, custkey, suppkey, datekey, quantity) VALUES (?, ?, ?, ?, ?)");	

		int cnt = 0;
		String line;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			int i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			if (atomCounter.incrementAndGet()%commitSize==0){
				//System.out.printf("Commit lineitem: %d\n", cnt);
				conn.commit();
			}
		}
		ps.close();
		conn.commit(true);
		conn.close();
		bufReader.close();
	}
	
	protected void insertLineItemTmpParallel(int size) throws Exception {
		ProducerConnection conn =  config.newRiTEProducerConnection(5434);
		//BufferedReader bufReader = config.getInsert(size); 
		//ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader = config.newDataSource(size);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO lineitemtmp(orderkey, custkey, suppkey, datekey, quantity) VALUES (?, ?, ?, ?, ?)");	

		int cnt = 0;
		String line;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			int i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			if (++cnt%commitSize==0){
				//System.out.printf("Commit lineitem: %d\n", cnt);
				conn.commit();
			}
		}
		ps.close();
		conn.commit(false);
		conn.close();
		bufReader.close();
	}
	
	protected void insertOrdersParallel(int size) throws Exception {
		ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/orders1000000.csv"), 16384);
		//ProducerConnection conn =  config.newRiTEProducerConnection(5433);
		//BufferedReader bufReader = config.newDataSource(size);
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
			if (atomCounter.incrementAndGet()%commitSize==0){
				//System.out.printf("Commit order: %d\n", cnt);
				conn.commit();
			}
		}
		ps.close();
		/*conn.commit(true);
		conn.close();*/
		bufReader.close();
	}
	
	protected void insertOrdersTmpParallel(int size) throws Exception {
		ProducerConnection conn =  config.getRiTEProducerConnection();
		//BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/orders1000000.csv"), 16384);
		//ProducerConnection conn =  config.newRiTEProducerConnection(5434);
		//BufferedReader bufReader = config.newDataSource(size);
		BufferedReader bufReader = config.getInsert(size);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		PreparedStatement ps = conn.prepareStatement("INSERT INTO orderstmp(orderkey, orderstatus, totalprice, orderdate, shippriority) values (?,?,?,?,?)");	
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
			if (atomCounter.incrementAndGet()%commitSize==0){
				//System.out.printf("Commit order: %d\n", cnt);
				conn.commit();
			}
		}
		ps.close();
		/*conn.commit(true);
		conn.close();
		bufReader.close();*/
	}
	
	
	
	protected void insertSupplierParallel() throws Exception { 
		ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/suppliers.csv"), 16384);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO suppliers(suppkey, name, address, nation, phone, acctbal) VALUES (?, ?, ?, ?, ?, ?)");	
		
		String line;
		int i;
		int cnt = 0;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setDouble(++i, Double.valueOf(vals[i - 1]));
			ps.execute();
			if (++cnt%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		bufReader.close();
	}

	protected void insertDateParallel() throws Exception {
		ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/dates.csv"), 16384);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO dates(datekey, daynr, monthnr, year) VALUES (?, ?, ?, ?)");	
		
		String line;
		int i;
		int cnt = 0;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.execute();
			if (++cnt%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		bufReader.close();
	}

	protected void insertCustomerParallel() throws Exception {
		ProducerConnection conn =  config.getRiTEProducerConnection();
		BufferedReader bufReader = new BufferedReader(new FileReader("/data1/allritedata/customers.csv"), 16384);
		if (lazyFlush) {
			conn.setFlusher(new LazyFlush());
		}
		
		PreparedStatement ps = conn.prepareStatement("INSERT INTO customers(custkey, name, address, email, nation, phone, acctbal) VALUES (?, ?, ?, ?, ?, ?, ?)");	
		
		String line;
		int i;
		while ((line = bufReader.readLine()) != null) {
			String[] vals = StringUtils.splitToArray(line, "|", false);
			i = 0;
			ps.setInt(++i, Integer.parseInt(vals[i - 1]));
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setString(++i, vals[i - 1]);
			ps.setDouble(++i, Double.valueOf(vals[i - 1]));
			ps.execute();
			if (atomCounter.incrementAndGet()%commitSize==0){
				conn.commit();
			}
		}
		ps.close();
		bufReader.close();
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
				case 2: insertOrdersParallel(size); break;
				case 3: insertDateParallel(); break;
				case 4: insertSupplierParallel(); break;
				case 5: insertOrdersTmpParallel(size); break;
				case 6: insertLineItemTmpParallel(size); break;
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
