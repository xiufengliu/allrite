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

import java.net.InetSocketAddress;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.producer.staging.TupleStore;
import dk.aau.cs.rite.sqlparser.ParseInfo;


public class RiteSelectPreparedStatement extends RitePreparedStatement {

	PreparedStatement jdbcPstmt;
	dk.aau.cs.rite.tuplestore.TupleStore memTupleStore;
	TupleStore stagingTupleStore;
	
	
	public  RiteSelectPreparedStatement(ProducerConnection riteConn, InetSocketAddress serverAddr, ParseInfo parseInfo, TupleStore tupleStore) throws SQLException{ 
		super(riteConn,serverAddr, parseInfo, tupleStore);
		this.jdbcPstmt = riteConn.jdbcConn.prepareStatement(parseInfo.getSql());
		this.stagingTupleStore = tupleStore;
		
	}
	
	@Override
	public ResultSet executeQuery() throws SQLException {
		try {
			tupleStore.materialize(); // Will materialize the committed rows in the archive and in the catalyst.
			return new dk.aau.cs.rite.producer.ResultSet(parseInfo, tupleStore, jdbcPstmt.executeQuery()); // Merge the query results from DBMS and the un-committed rows.
		} catch (RiteException e) {
			e.printStackTrace();
			throw new SQLException(e.getCause());
		}
	}


	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		jdbcPstmt.setNull(parameterIndex, sqlType);
	}


	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		jdbcPstmt.setInt(parameterIndex, x);
		
	}


	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		jdbcPstmt.setLong(parameterIndex, x);
		
	}


	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		jdbcPstmt.setFloat(parameterIndex, x);
		
	}


	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		jdbcPstmt.setDouble(parameterIndex, x);
		
	}


	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		jdbcPstmt.setString(parameterIndex, x);
		
	}


	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		jdbcPstmt.setDate(parameterIndex, x);
		
	}

	@Override
	public void close() throws SQLException{
		jdbcPstmt.close();
		super.close();
	}
	
	
}
