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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import dk.aau.cs.rite.common.CmdLineParser;
import dk.aau.cs.rite.common.TimeTracer;
import dk.aau.cs.rite.common.Utils;

public class TestMain {

	TimeTracer thiz = TimeTracer.thiz();
	
	Map<Integer, Test> tests = new HashMap<Integer, Test>();
	Config config;
	
	public TestMain(Config config) {
		this.config = config;
		if (config.booleanValue(config.TEST_MULTI_TABLE)){
			if (config.booleanValue(config.TEST_TRADJDBC))
				tests.put(0,  new MultiTableByTradJDBC(config));
			if (config.booleanValue(config.TEST_JDBCBATCH))
				tests.put(1,  new MultiTableByBatch(config));
			if (config.booleanValue(config.TEST_BULKLOAD))
				tests.put(2,  new MultiTableByBulkload(config));
			if (config.booleanValue(config.TEST_INSTANTNOMAT))
				tests.put(3,  new MultiTableByRITE(config, false));
			if (config.booleanValue(config.TEST_INSTANTWITHMAT))
				tests.put(4, 	new MultiTableByRITE(config, false));
			if (config.booleanValue(config.TEST_LAZYNOMAT))
				tests.put(5, 	new MultiTableByRITE(config, true));
			if (config.booleanValue(config.TEST_LAZYWITHMAT))
				tests.put(6,  new MultiTableByRITE(config, true));
		} else { // RiTEJDBC(Config config,  boolean lazyFlush, boolean commit)
			if (config.booleanValue(config.TEST_TRADJDBC))
				tests.put(0,  new TradJDBC(config));
			if (config.booleanValue(config.TEST_JDBCBATCH))
				tests.put(1,  new JDBCBatch(config));
			if (config.booleanValue(config.TEST_BULKLOAD))
				tests.put(2,  new Bulkload(config));
			if (config.booleanValue(config.TEST_INSTANTNOMAT))
				tests.put(3,  new RiTEJDBC(config,  false, false));
			if (config.booleanValue(config.TEST_INSTANTWITHMAT))
				tests.put(4, 	new RiTEJDBC(config, false, true));
			if (config.booleanValue(config.TEST_LAZYNOMAT))
				tests.put(5, 	new RiTEJDBC(config, true, false));
			if (config.booleanValue(config.TEST_LAZYWITHMAT))
				tests.put(6,  new RiTEJDBC(config,  true, true));
		}
	}

	public void run() {
		int startSize = config.getStartSize();
		int endSize = config.getEndSize();
		for (int fileSize = startSize; fileSize <= endSize; ++fileSize) {
			for (Integer id : tests.keySet()) {
				clearTable();
				thiz.start(String.valueOf(id));
				this.load(id, fileSize);
				thiz.end(String.valueOf(id), config.getNameByID(id));
			}
		}
	}

	protected void load(int id, int fileSize) {
		Test t = tests.get(id);
		String name = config.getNameByID(id);
		try {
			if ("updatedelete".equals(config.getOP())) {
				System.out.printf("Loading %s, update and delete on-the-fly\n", name);
				t.updel(fileSize);
			} else if ("insert".equals(config.getOP()))	{
				System.out.printf("Loading %s, insert only\n", name);
				t.insert(fileSize);
			} else if ("update".equals(config.getOP()))	{
				System.out.printf("Loading %s, update on-the-fly\n", name);
				t.update(fileSize);
			} else if ("delete".equals(config.getOP()))	{
				System.out.printf("Loading %s, delete on-the-fly\n", name);
				t.delete(fileSize);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			thiz.start("lastcommit");
			config.close();
			thiz.end("lastcommit", "Materialize " + name);
		}
	}

	protected void clearTable() {
		Connection jdbcConn = null;
		try {
			jdbcConn = config.getPostgreSQLConnection();
			jdbcConn.setAutoCommit(true);
			String sql = "TRUNCATE "+config.getTableName();
			System.out.println(sql);
			PreparedStatement ps = jdbcConn	.prepareStatement(sql);
			ps.execute();
			ps = jdbcConn.prepareStatement("TRUNCATE orders");
			ps.execute();
			ps = jdbcConn.prepareStatement("TRUNCATE lineitem");
			ps.execute();
			ps = jdbcConn.prepareStatement("TRUNCATE customers");
			ps.execute();
			ps = jdbcConn.prepareStatement("TRUNCATE dates");
			ps.execute();
			ps = jdbcConn.prepareStatement("TRUNCATE suppliers");
			ps.execute();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static  void restartServer() {
		List<String> cmds = new ArrayList<String>();
		cmds.add("./restartserver.sh");
		try {
			Utils.execShellCmd(cmds, "/home/xiliu/workspace/rite/test", false);
			Thread.sleep(5*1000);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
	private static void printUsage() {
        System.err.println("Usage: TestMain -n [data size] -m [commit size]");
    }
	
	public static void main(String[] args) {
		
		
		CmdLineParser parser = new CmdLineParser();
		CmdLineParser.Option op = parser.addIntegerOption('t', "operation type");
		CmdLineParser.Option size = parser.addIntegerOption('n', "data size");
		CmdLineParser.Option commit = parser.addIntegerOption('m', "commit size");
		CmdLineParser.Option rangeValue = parser.addIntegerOption('r', "range value");
		CmdLineParser.Option dataFreshnessArg = parser.addIntegerOption('f', "data freshness");
		
		try {
            parser.parse(args);
            Integer type =(Integer) parser.getOptionValue(op, new Integer(-1));
            Integer dataSize =(Integer) parser.getOptionValue(size, new Integer(-1));
            Integer commitSize =(Integer)parser.getOptionValue(commit, new Integer(-1));
            Integer rValue =(Integer)parser.getOptionValue(rangeValue, new Integer(-1));
            Integer dataFreshness =(Integer)parser.getOptionValue(dataFreshnessArg, new Integer(-1));
            Config config = new Config();
            
            config.setStartSize(dataSize);
            config.setEndSize(dataSize);
            config.setCommitSize(commitSize);
            config.setDataFreshness(dataFreshness);
            
            TestMain t = new TestMain(config);
            TimeTracer thiz = TimeTracer.thiz();
    		
            t.run();
    		
        } catch ( CmdLineParser.OptionException e ) {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(2);
        }
	}
}
