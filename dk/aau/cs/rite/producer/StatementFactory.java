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
import java.sql.SQLException;

import dk.aau.cs.rite.producer.staging.TupleStore;
import dk.aau.cs.rite.sqlparser.ParseInfo;

public class StatementFactory {

	public static RitePreparedStatement getPreparedStatement(ProducerConnection riteConn, InetSocketAddress serverAddr, ParseInfo parseInfo, TupleStore tupleStore) throws SQLException {
	
		switch (parseInfo.getStatementType()) {
		case INSERT:
			return new RiteInsertPreparedStatement(riteConn, serverAddr, parseInfo,  tupleStore);
		case UPDATE:
			return new RiteUpdateAndDeletePreparedStatement(riteConn, serverAddr, parseInfo, tupleStore);
		case DELETE:
			return new RiteUpdateAndDeletePreparedStatement(riteConn, serverAddr, parseInfo, tupleStore);
		case SELECT:
			return new RiteSelectPreparedStatement(riteConn, serverAddr, parseInfo, tupleStore);
		default:
			return null;
		}
	}
}
