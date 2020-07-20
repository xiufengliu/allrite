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

package dk.aau.cs.rite;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import dk.aau.cs.rite.consumer.ConsumerConnection;
import dk.aau.cs.rite.producer.ProducerConnection;

public class RiteDriverManager {

	public static Connection getConnection(ConnectionType type,
			String jdbcDriver, String jdbcUrl, String dbUsername,
			String dbPassword, String server, int port) throws SQLException {
		try {
			InetSocketAddress serverAddr = new InetSocketAddress(server, port);
			Class.forName(jdbcDriver);
			Connection jdbcConn = DriverManager.getConnection(jdbcUrl,
					dbUsername, dbPassword);

			if (type == ConnectionType.PRODUCER) {
				return new ProducerConnection(jdbcConn, serverAddr, jdbcDriver,
						jdbcUrl, dbUsername, dbPassword);
			} else {
				jdbcConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				jdbcConn.setAutoCommit(false);
				return new ConsumerConnection(jdbcConn, serverAddr);
			}
		} catch (ClassNotFoundException e) {
			throw new SQLException(jdbcDriver + " is not found!", e);
		} catch (IOException e) {
			throw new SQLException("Failed to connect to the catalyst server!",	e);
		}
	}
}
