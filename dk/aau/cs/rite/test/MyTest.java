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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.tuplestore.Counter;
import dk.aau.cs.rite.tuplestore.Locker;
import dk.aau.cs.rite.tuplestore.ReadWriteMap;
import dk.aau.cs.rite.tuplestore.Segment;
import dk.aau.cs.rite.tuplestore.ud.AlgRel;
public class MyTest  {

	
	Logger log = Logger.getLogger(MyTest.class.getName());
	/**
	 * @param args
	 */
	
	public void testDumpTextfile() {
		try {
			File file = new File("/tmp/dump.csv");
			FileChannel fc = new FileOutputStream(file).getChannel();
			ByteBuffer buf = ByteBuffer.allocate(1024);
			buf.put(Integer.toString(12).getBytes()).put(Utils.getBytesUtf8("|")).put(Utils.getBytesUtf8("Hello")).put(Utils.getBytesUtf8("|")).put(Long.toString(100).getBytes());
			buf.flip();
			fc.write(buf);
			buf.clear();
			fc.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void testQueueOrder() {

	}

	public void testNegvataion() {
		TreeMap<Integer, Integer> rowIndex = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Counter> registerCounter = new TreeMap<Integer, Counter>();
		TreeMap<Integer, String> segCache = new TreeMap<Integer, String>();
		TreeMap<Long, Integer> timeIndex = new TreeMap<Long, Integer>();
		
		
		rowIndex.put(0, 0); registerCounter.put(0, new Counter()); segCache.put(0, "seg-0.dat");
		rowIndex.put(2, 1); registerCounter.put(2, new Counter()); segCache.put(1, "seg-1.dat"); timeIndex.put(1306827145100L, 1);
		rowIndex.put(4, 2); registerCounter.put(4, new Counter()); segCache.put(2, "seg-2.dat"); timeIndex.put(1306827146208L, 2);
		rowIndex.put(6, 3); registerCounter.put(6, new Counter()); segCache.put(3, "seg-3.dat"); timeIndex.put(1306827146801L, 5);
		
		
		long now = 1306827147801L;
		
		
		log.info(String.valueOf(Long.MAX_VALUE));
	}

	
	public void testObjectCompare(){
		Object[] obj = new Object[2];
		obj[0] = null;
		obj[1] = 1;
		System.out.println(Utils.compare(obj[0], obj[1]));
	}
	
	public void testStringSplit(){
		String whereClause = "select * from tablename";
		
		String[]strs = whereClause.split("where");
		System.out.println(strs.length);
		for (String s : strs){
			System.out.println(s);
		}
	}
	
	public void testEnum(){
		System.out.println(AlgRel.values()[2]);
	}
	public void testNullInArrayList(){
		List<String> cols = new ArrayList<String>();
		cols.add(null);
		cols.add(null);
		System.out.println(cols.size());
	}
	
	public void testReturn(){
		try{
		while (true){
			return;
		}
		} catch (Exception e){
			e.printStackTrace();
		} finally{
			System.out.println("Hello");
		}
	}
	
	public void testBoolean(Boolean [] bb, int j){
		if (j==2)
		bb[0] = true;
	}
	
	
	public  void restartServer() {
		List<String> cmds = new ArrayList<String>();
		cmds.add("./rite.sh");
		try {
			Utils.execShellCmd(cmds, "/home/xiliu/workspace/rite/test", false);

			 for (int i = 0; i < 5; i++) {
		            //Pause for 4 seconds
		            Thread.sleep(4000);
		            //Print a message
		            System.out.println("aaa");
		        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void testArrayCopy(){
    int[] types = {1, 23, 4, 5};
    
    int[] newTypes = new int[types.length+1];
	System.arraycopy(types, 0, newTypes, 0, types.length);
	newTypes[types.length] = java.sql.Types.INTEGER;
	for(int i : newTypes)
		System.out.println(i);
	
	}
	
	public void testCopy() {
		try {
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection("jdbc:postgresql://localhost/xiliu", "xiliu", "abcd1234");
			Statement stmt = con.createStatement();
			boolean success = stmt.execute("COPY orders FROM '/data1/allritedata/orders_fifo' WITH DELIMITER '|' NULL AS ''");
			System.out.println("Execute Success= " + success);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String removeDuplicateChar(String str){
		boolean[] flags = new boolean[256];
			StringBuffer strBuf = new StringBuffer();
			for (int i=0; i<str.length(); ++i){
				char ch = str.charAt(i);
				if (!flags[ch]){
					strBuf.append(ch);
					flags[ch] = true;
				}
			}
		return strBuf.toString();
	}
	
	public int []removeElementAppearOnce(int []nums){
		int [] rets = new int[nums.length-1];
		
		
		return rets;
	}
	
	
	
	
	public void order(String str){
		boolean[] flags = new boolean[128];
		int total = 0;
		for (int i =0; i<str.length(); ++i){
			char c = str.charAt(i);
	if ((c-'0')<=9){
		total += c - '0';
	} else {
		flags[c] = true;
	}				
	}
	StringBuffer buf = new StringBuffer();
	for (int i=0; i<flags.length; ++i){
		if (flags[i]){
			buf.append((char) i);
	}
	}
	buf.append(total);
	System.out.println(buf.toString());
	}
	
	public int fibbonacci(int n){
		if (n==0)	return 0;

	if (n==1) return 1;


	return fibbonacci(n-1) + fibbonacci(n-2);

	}
	
	public static void main(String[] args) {
		MyTest test = new MyTest();
		
		
		System.out.println(test.fibbonacci(7));
	}
		
	
	public void testSoftReference(){
		ReadWriteMap<Integer, SoftReference<Segment>> segCache = new ReadWriteMap<Integer, SoftReference<Segment>>();
		NavigableMap<Integer, SoftReference<Segment>> rMap = segCache.getReadableMap();
		rMap.put(1, new SoftReference<Segment>(null));
		SoftReference<Segment> sr = segCache.getFromRMap(1);
		Segment seg = sr.get();
		
		
	}
	
	
	int value = 0;
	boolean running = true;
	synchronized public void printInfo1(){
		while(true){
			++value;
			System.out.println("Thread1: value="+value);
		}
	}

	 public void printInfo2(){
		
		System.out.println("Thread2: value="+value);
		
	}
	
	
}

class Printer implements Runnable {
	MyTest t;
	int idx;
	public Printer(MyTest t, int idx){
		this.t = t;
		this.idx = idx;
	}
	@Override
	public void run() {
		if (this.idx==1) {
			   t.printInfo1();
		}	else
			while(true)
			   t.printInfo2();
	}
}


