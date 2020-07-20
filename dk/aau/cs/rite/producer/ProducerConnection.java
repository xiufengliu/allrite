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

package dk.aau.cs.rite.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.producer.flush.FlushTicker;
import dk.aau.cs.rite.producer.flush.IFlush;
import dk.aau.cs.rite.producer.flush.InstantFlush;
import dk.aau.cs.rite.producer.flush.LazyFlush;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.producer.staging.ITransfer;
import dk.aau.cs.rite.producer.staging.InstantTransfer;
import dk.aau.cs.rite.producer.staging.LazyTransfer;
import dk.aau.cs.rite.producer.staging.StagingTupleStore;
import dk.aau.cs.rite.producer.staging.TupleStore;
import dk.aau.cs.rite.sqlparser.ParseInfo;
import dk.aau.cs.rite.sqlparser.SqlParser;

public class ProducerConnection implements Connection{
	
	 Connection jdbcConn;
	 List<Statement> stmts;
	 Map<String, Catalog> catalogs;
	 Map<String, TupleStore> tupleStores; // The staging tuple stores in the producer side.
	 InetSocketAddress serverAddr;
	 FlushTicker flushTicker;
	 boolean materializedOnCommit;
	 protected String jdbcDriver, jdbcUrl, dbUsername, dbPassword; // Will be sent to the mem server.
	
	public ProducerConnection(Connection jdbcConn, InetSocketAddress serverAddr, String jdbcDriver, String jdbcUrl, String dbUsername, String dbPassword) throws IOException {
		this.jdbcConn = jdbcConn;
		this.jdbcDriver = jdbcDriver;
		this.jdbcUrl = jdbcUrl;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;
		
		this.stmts = new ArrayList<Statement>();
		this.serverAddr = serverAddr;
		
		
		this.catalogs = new HashMap<String, Catalog>();
		this.tupleStores = new HashMap<String, TupleStore>();
		this.materializedOnCommit = false;
	}

	@Override
	synchronized public PreparedStatement prepareStatement(String sql) throws SQLException {
		ParseInfo parseInfo = SqlParser.parse(sql);
		
		Catalog catalog = ensureCatalog(parseInfo.getTableName()); // Create catalog on producer and in the server.
		TupleStore tupleStore = ensureTupleStore(catalog);
		PreparedStatement pstmt = StatementFactory.getPreparedStatement(this, serverAddr, parseInfo, tupleStore);
		stmts.add(pstmt);
		return pstmt;
	}
	
	synchronized public void setFlusher(IFlush flusher) throws IOException {
		if ((this.flushTicker==null) && (flusher instanceof LazyFlush)) {// Default is InstantFlush, no need to set.
			this.flushTicker = new FlushTicker(flusher, serverAddr);
		}
	}
	
	synchronized protected TupleStore ensureTupleStore(Catalog catalog) throws SQLException {
		try {
			String tableName = catalog.getTableName();
			TupleStore tupleStore = tupleStores.get(tableName);
			if (tupleStore == null) {
				ITransfer transfer;
				if (this.flushTicker != null) {
					transfer = new LazyTransfer(serverAddr, catalog);
					this.flushTicker.addTransfer(tableName, transfer);
				} else {
					transfer = new InstantTransfer(serverAddr, catalog);
				}
				tupleStore = new StagingTupleStore(transfer, catalog); // Create a staging tuple store in the producer side.
				tupleStores.put(tableName, tupleStore);
			}
			return tupleStore;
		} catch (IOException e) {
			throw new SQLException("Cannot connet to the memory server!");
		}
	}

	protected Catalog ensureCatalog(String tableName) throws SQLException {
		try {
			Catalog catalog = catalogs.get(tableName);
			if (catalog == null) {
				catalog = new Catalog(tableName);
				DatabaseMetaData metadata = jdbcConn.getMetaData();
				ResultSet rs = metadata.getColumns(null, null, tableName, null);
				ResultSet primRs = metadata.getPrimaryKeys(null, null,tableName);
				while (rs.next()) {
					catalog.addColType(rs.getString("COLUMN_NAME")
							.toLowerCase(), rs.getInt("DATA_TYPE"));
				}

				while (primRs.next()) {
					catalog.addPrimarykey(primRs.getString("COLUMN_NAME")
							.toLowerCase());
				}
				catalog.setTargetJdbcInfo(jdbcDriver, jdbcUrl, dbUsername, dbPassword);
				catalog.syncWithCatalyst(this.serverAddr); // Sync the catalog with the catalyst in the catalyst. 
				catalogs.put(tableName, catalog);
			}
			return catalog;
		} catch (IOException e) {
			throw new SQLException("Failed to create catalog in server!");
		}
	}

	@Override
	synchronized public void commit() throws SQLException {
		commit(false);
	}


	synchronized public void commit(boolean materializedOnCommit) throws SQLException {
		try {
			for (TupleStore tupleStore : tupleStores.values()) {
				tupleStore.commit(materializedOnCommit);
			}
		} catch (RiteException e) {
			throw new SQLException(e);
		}
	}
	
	@Override
	synchronized public void rollback() throws SQLException {
		try {
			for (TupleStore tupleStore : tupleStores.values()) {
				tupleStore.rollback();
			}
		} catch (RiteException e) {
			throw new SQLException("Failed to rollback!");
		}
    }
	
	
	@Override
	synchronized public void close() throws SQLException {
		try {
			if (flushTicker != null) {// Lazy Commit.
				this.flushTicker.close();
			}
			for (TupleStore tupleStore : tupleStores.values()) {
				tupleStore.close();
			}
			jdbcConn.close();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
	
	
	public Connection getJDBCConnection(){
		return this.jdbcConn;
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return null;
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		return null;
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		return null;
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCatalog() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}



	
	
	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}



}
