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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.server.ServerCommand;
import dk.aau.cs.rite.sqlparser.ParseInfo;
import dk.aau.cs.rite.sqlparser.SqlParser;


public class RitePreparedStatement implements PreparedStatement {
  
	Connection jdbcConn;
	InetSocketAddress serverAddr;
	PreparedStatement sqlStmt;
	PreparedStatement startTimeStmt;
	ParseInfo parseInfo;
	ByteChannel channel;
	ByteBuffer buffer;
	long freshness;
	
	public RitePreparedStatement(Connection jdbcConn, InetSocketAddress serverAddr, String sql, int resultSetType,	int resultSetConcurrency)	throws SQLException {
		try {
			this.jdbcConn = jdbcConn;
			this.serverAddr = serverAddr;

			this.channel = SocketChannel.open(serverAddr);
			this.buffer = ByteBuffer.allocate(1024);
			
			this.parseInfo = SqlParser.parse(sql);
			this.sqlStmt = jdbcConn.prepareStatement(sql, resultSetType, resultSetConcurrency);
			this.freshness = 0;
		} catch (IOException e) {
			throw new SQLException("Cannot connect to the server!");
		}
	}
	
	public RitePreparedStatement(Connection jdbcConn, InetSocketAddress serverAddr, String sql)	throws SQLException {
		this(jdbcConn, serverAddr, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}
	
	public void ensureAccuracy(long freshness, TimeUnit unit) throws SQLException {
		this.ensureAccuracy(TimeUnit.MILLISECONDS.convert(freshness, unit));
	}

	public void ensureAccuracy(long freshness) throws SQLException {
		this.freshness =  freshness;
	}

	protected int[] register(String tableName) throws SQLException {
		int result = -1;
		try {
			PreparedStatement minMaxPstmt = jdbcConn.prepareStatement("select min, max from rite_minmax where name=?");
			minMaxPstmt.setString(1, tableName);
			byte[]tblBytes = Utils.getBytesUtf8(tableName);
			
			while (true) {
				ResultSet rs = minMaxPstmt.executeQuery();
				int min = 0;
				int max = 1;
				if (rs.next()) {
					min = rs.getInt(1);
					max = rs.getInt(2);
				}

				// The server communication
				buffer.clear();
				buffer.putInt(ServerCommand.CUST_REGISTER_QUERY.ordinal())
					  .putInt(tblBytes.length).put(tblBytes).putInt(min)
					  .putInt(max).putLong(freshness).flip();
				channel.write(buffer);

				buffer.clear();
				channel.read(buffer);
				result = buffer.getInt(0);
				
				if (result == ServerCommand.OK.ordinal()) // Register success
					return new int[]{min, max};
				else
					jdbcConn.commit(); // Register fail,  start new transaction and register again
			}
		} catch (IOException e) {
			throw new SQLException("Cannot register query", e);
		} 
	}
	
	protected void setQueryStartTime() throws SQLException {
		if (startTimeStmt==null||startTimeStmt.isClosed()){
			long queryStartTime = System.currentTimeMillis();
			this.startTimeStmt = jdbcConn.prepareStatement("SET rite.timehandle TO "+ queryStartTime);
		}
		startTimeStmt.execute();
	}
	
	protected void unregister(String tableName, int[] minMax) throws SQLException {
		try {
			jdbcConn.commit();
			byte[] tblBytes = Utils.getBytesUtf8(tableName);
			buffer.clear();
			buffer.putInt(ServerCommand.CUST_UNREGISTER_QUERY.ordinal())
					.putInt(tblBytes.length).put(tblBytes).putInt(minMax[0])
					.putInt(minMax[1]).flip();
			channel.write(buffer);
		} catch (IOException e) {
			throw new SQLException("Cannot unregister query", e);
		}
	}

	protected void closeSocket() {
		try {
			buffer.clear();
			buffer.putInt(ServerCommand.BYE.ordinal()).flip();
			channel.write(buffer);
		} catch (IOException e) {
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		int[] minMax = this.register(parseInfo.getTableName());
		this.setQueryStartTime();
		ResultSet rs = sqlStmt.executeQuery();
		this.unregister(parseInfo.getTableName(), minMax);
		return rs;
	}
	
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		parseInfo = SqlParser.parse(sql);
		return executeQuery();
	}


	@Override
	public int executeUpdate(String sql) throws SQLException {
		return sqlStmt.executeUpdate(sql);
	}


	@Override
	public void close() throws SQLException {
		closeSocket();
		startTimeStmt.close();
		sqlStmt.close();
	}


	@Override
	public int getMaxFieldSize() throws SQLException {
		return sqlStmt.getMaxFieldSize();
	}


	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		sqlStmt.setMaxFieldSize(max);
	}


	@Override
	public int getMaxRows() throws SQLException {
		return sqlStmt.getMaxRows();
	}


	@Override
	public void setMaxRows(int max) throws SQLException {
		sqlStmt.setMaxRows(max);
	}


	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		sqlStmt.setEscapeProcessing(enable);
	}


	@Override
	public int getQueryTimeout() throws SQLException {
		return sqlStmt.getQueryTimeout();
	}


	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		sqlStmt.setQueryTimeout(seconds);
		
	}


	@Override
	public void cancel() throws SQLException {
		sqlStmt.cancel();
		
	}


	@Override
	public SQLWarning getWarnings() throws SQLException {
		return sqlStmt.getWarnings();
	}


	@Override
	public void clearWarnings() throws SQLException {
		sqlStmt.clearWarnings();
		
	}


	@Override
	public void setCursorName(String name) throws SQLException {
		sqlStmt.setCursorName(name);
	}


	@Override
	public boolean execute(String sql) throws SQLException {
		return sqlStmt.execute(sql);
	}


	@Override
	public ResultSet getResultSet() throws SQLException {	
		return sqlStmt.getResultSet();
	}


	@Override
	public int getUpdateCount() throws SQLException {
		return 0;
	}


	@Override
	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void setFetchDirection(int direction) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int getResultSetConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int getResultSetType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void addBatch(String sql) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void clearBatch() throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean isPoolable() throws SQLException {
		return sqlStmt.isPoolable();
	}


	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return sqlStmt.unwrap(iface);
	}


	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return sqlStmt.isWrapperFor(iface);
	}


	@Override
	public int executeUpdate() throws SQLException {
		return sqlStmt.executeUpdate();
	}


	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		sqlStmt.setNull(parameterIndex, sqlType);
	}


	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		sqlStmt.setBoolean(parameterIndex, x);
		
	}


	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		sqlStmt.setByte(parameterIndex, x);
		
	}


	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		sqlStmt.setShort(parameterIndex, x);
	}


	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		sqlStmt.setInt(parameterIndex, x);
	}


	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		sqlStmt.setLong(parameterIndex, x);
		
	}


	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		sqlStmt.setFloat(parameterIndex, x);
		
	}


	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		sqlStmt.setDouble(parameterIndex, x);
		
	}


	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		sqlStmt.setBigDecimal(parameterIndex, x);
		
	}


	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		sqlStmt.setString(parameterIndex, x);
		
	}


	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		sqlStmt.setBytes(parameterIndex, x);
		
	}


	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		sqlStmt.setDate(parameterIndex, x);
		
	}


	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		sqlStmt.setTime(parameterIndex, x);
		
	}


	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		sqlStmt.setTimestamp(parameterIndex, x);
		
	}


	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		sqlStmt.setAsciiStream(parameterIndex, x);
		
	}


	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		sqlStmt.setUnicodeStream(parameterIndex, x, length);
		
	}


	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		sqlStmt.setBinaryStream(parameterIndex, x, length);
		
	}


	@Override
	public void clearParameters() throws SQLException {
		sqlStmt.clearParameters();
		
	}


	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		sqlStmt.setObject(parameterIndex, x, targetSqlType);
		
	}


	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		sqlStmt.setObject(parameterIndex, x);
		
	}


	@Override
	public boolean execute() throws SQLException {
		return sqlStmt.execute();
	}


	@Override
	public void addBatch() throws SQLException {
		sqlStmt.addBatch();
		
	}


	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		sqlStmt.setCharacterStream(parameterIndex, reader, length);
		
		
	}


	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		sqlStmt.setRef(parameterIndex, x);
		
	}


	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		sqlStmt.setBlob(parameterIndex, x);
		
	}


	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		sqlStmt.setClob(parameterIndex, x);
		
	}


	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		sqlStmt.setArray(parameterIndex, x);
		
	}


	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return sqlStmt.getMetaData();
	}


	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		sqlStmt.setDate(parameterIndex, x, cal);
		
	}


	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		sqlStmt.setTime(parameterIndex, x, cal);
		
	}


	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		sqlStmt.setTimestamp(parameterIndex, x, cal);
		
	}


	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		sqlStmt.setNull(parameterIndex, sqlType, typeName);
		
	}


	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		sqlStmt.setURL(parameterIndex, x);
		
	}


	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
	
		return sqlStmt.getParameterMetaData();
	}


	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		sqlStmt.setRowId(parameterIndex, x);
		
	}


	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		sqlStmt.setNString(parameterIndex, value);
		
	}


	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		sqlStmt.setNCharacterStream(parameterIndex, value, length);
		
	}


	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub
		
	}

    
        
}
