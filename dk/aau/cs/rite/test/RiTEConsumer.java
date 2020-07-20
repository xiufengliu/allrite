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

import java.sql.ResultSet;
import java.sql.SQLException;

import dk.aau.cs.rite.consumer.ConsumerConnection;
import dk.aau.cs.rite.consumer.RitePreparedStatement;

public class RiTEConsumer implements Test {
	Config config;
	private ConsumerConnection conn;
	private RitePreparedStatement ps;

	public RiTEConsumer(Config config) {
		this.config = config;
	}

	protected void realtime() {
		try {
			conn = config.getRiTEConsumerConnection();
			String tableName = config.getTableName();

			String sql = "select * from lineitem where 0=1 union select * from tablefunction('"
					+ tableName
					+ "', (select min from rite_minmax where name='"
					+ tableName
					+ "'), "
					+ "(select max from rite_minmax where name='"
					+ tableName
					+ "'))";

			ps = (RitePreparedStatement) conn.prepareStatement(sql,
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			ResultSet rs = ps.executeQuery();

			rs.last();
			int count = rs.getRow();
			rs.beforeFirst();

			System.out.println(count);

			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void righttime(long freshness) {
		try {
			conn = config.getRiTEConsumerConnection();
			String tableName = config.getTableName();

			String sql = "select * from lineitem where 0=1 union select * from tablefunction('"
					+ tableName
					+ "', (select min from rite_minmax where name='"
					+ tableName
					+ "'), "
					+ "(select max from rite_minmax where name='"
					+ tableName
					+ "'))";
			
			ps = (RitePreparedStatement) conn.prepareStatement(sql,
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			
			ps.ensureAccuracy(freshness);
			ResultSet rs = ps.executeQuery();

			rs.last();
			int count = rs.getRow();
			rs.beforeFirst();

			/*
			 * StringBuilder row = new StringBuilder(); while (rs.next()) {
			 * row.setLength(0); row.append("("); for (int i = 1; i <= 6; ++i) {
			 * row.append(rs.getInt(i)); if (i != 6) row.append(","); }
			 * row.append(")\n"); System.out.println(row.toString());
			 * 
			 * }
			 */

			System.out.println(count);

			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Config config = new Config();
		RiTEConsumer consumer = new RiTEConsumer(config);
		consumer.select();
		config.close();

	}

	@Override
	public void insert(int size) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updel(int size) {
		// TODO Auto-generated method stub

	}

	@Override
	public void select() {
		int freshness = config.getDataFreshness();
		if (freshness > 0) {
			righttime(freshness);
		} else {
			realtime();
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
