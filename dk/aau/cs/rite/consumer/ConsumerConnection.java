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

package dk.aau.cs.rite.consumer;

import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;


public class ConsumerConnection implements Connection {

	Connection jdbcConn;
	InetSocketAddress serverAddr;
	
	
	public ConsumerConnection(Connection jdbcConn, InetSocketAddress serverAddr) {
		this.jdbcConn = jdbcConn;
		this.serverAddr = serverAddr;
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new RitePreparedStatement(jdbcConn, serverAddr, sql);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return new RitePreparedStatement(jdbcConn, serverAddr, sql, resultSetType, resultSetConcurrency);
	}

		
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return jdbcConn.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return jdbcConn.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return jdbcConn.createStatement();
	}
             
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return jdbcConn.prepareCall(sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return jdbcConn.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		jdbcConn.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return jdbcConn.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		this.jdbcConn.commit();
	}

	@Override
	public void rollback() throws SQLException {
		this.jdbcConn.rollback();
		
	}

	@Override
	public void close() throws SQLException {
		this.jdbcConn.close();
		
	}

	@Override
	public boolean isClosed() throws SQLException {
		return jdbcConn.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return jdbcConn.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		jdbcConn.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return jdbcConn.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		jdbcConn.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return jdbcConn.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		jdbcConn.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return jdbcConn.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return jdbcConn.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		jdbcConn.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return jdbcConn.createStatement(resultSetType, resultSetConcurrency);
	}


	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return jdbcConn.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return jdbcConn.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		jdbcConn.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		jdbcConn.setHoldability(holdability);
		
	}

	@Override
	public int getHoldability() throws SQLException {
			return jdbcConn.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return jdbcConn.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return jdbcConn.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		jdbcConn.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		jdbcConn.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return jdbcConn.createStatement(resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return jdbcConn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return jdbcConn.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return jdbcConn.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return jdbcConn.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return jdbcConn.prepareStatement(sql, columnNames);
	}

	@Override
	public Clob createClob() throws SQLException {
		return jdbcConn.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return jdbcConn.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return jdbcConn.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return jdbcConn.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return jdbcConn.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		jdbcConn.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		jdbcConn.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return jdbcConn.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return jdbcConn.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return jdbcConn.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return jdbcConn.createStruct(typeName, attributes);
	}

	
}
