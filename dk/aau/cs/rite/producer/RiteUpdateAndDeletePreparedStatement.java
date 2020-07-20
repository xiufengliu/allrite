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
import java.sql.SQLException;
import java.sql.Types;

import dk.aau.cs.rite.producer.staging.TupleStore;
import dk.aau.cs.rite.sqlparser.ParseInfo;

public class RiteUpdateAndDeletePreparedStatement extends RitePreparedStatement {


	public RiteUpdateAndDeletePreparedStatement(ProducerConnection riteConn, 
			InetSocketAddress serverAddr, ParseInfo parseInfo,
			TupleStore tupleStore) throws SQLException {
		super(riteConn, serverAddr, parseInfo, tupleStore);

	}
	
	@Override
	public void setInt(int paramIndex, int value) throws SQLException {
		if(paramTypes[paramIndex] != Types.INTEGER)
	            throw new SQLException("The types are not compatible");
	        paramValues[paramIndex] = value;
	}

    @Override
    public void setDouble(int paramIndex, double value) throws SQLException {
        if(paramTypes[paramIndex] != Types.DOUBLE 
          && paramTypes[paramIndex] != Types.FLOAT 
          && paramTypes[paramIndex] != Types.NUMERIC)
            throw new SQLException("The types are not compatible");
        paramValues[paramIndex] = value;
    }

    @Override
    public void setDate(int paramIndex, Date value) throws SQLException {
        if(paramTypes[paramIndex] != Types.DATE)
            throw new SQLException("The types are not compatible");
        paramValues[paramIndex] = value;
    }

    @Override
    public void setLong(int paramIndex, long value) throws SQLException {
        if(paramTypes[paramIndex] != Types.BIGINT)
            throw new SQLException("The types are not compatible");
        paramValues[paramIndex] = value;
    }

    @Override
    public void setFloat(int paramIndex, float value) throws SQLException {
        if(paramTypes[paramIndex] != Types.REAL)
            throw new SQLException("The types are not compatible");
        paramValues[paramIndex] = value;
    }

    @Override
    public void setString(int paramIndex, String value) throws SQLException {
        if(paramTypes[paramIndex] != Types.VARCHAR 
        && paramTypes[paramIndex] != Types.LONGVARCHAR) // TODO: Add the remaining "string types"
            throw new SQLException("The types are not compatible");
        paramValues[paramIndex] = value;
    }
	
	@Override
	public void setNull(int paramIndex, int sqlType) throws SQLException {
		paramValues[paramIndex] = null;
	}
	
	


	@Override
	public boolean execute() throws SQLException {
		try {
			parseInfo.setParamValues(paramValues);
			tupleStore.updateAndDelete(parseInfo);
			super.clearParameters();
			return true;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
