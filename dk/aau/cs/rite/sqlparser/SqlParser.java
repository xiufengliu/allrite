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

package dk.aau.cs.rite.sqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dk.aau.cs.rite.common.StringUtils;
import dk.aau.cs.rite.tuplestore.ud.AlgRel;
import dk.aau.cs.rite.tuplestore.ud.Expression;

public class SqlParser implements ISqlParserConstants {

	ParseInfo parseInfo;

	protected ParseInfo parseSql(String sqlStr) throws SQLException {
		if (sqlStr == null) {
			throw new SQLException("SQL is null");
		}
		parseInfo = new ParseInfo();
		String sql = preProcessSQL(sqlStr);
		
		switch (sql.charAt(0)) {
		case 'i':
			parseInsert(sql);
			break;
		case 'd':
			parseDelete(sql);
			break;
		case 'u':
			parseUpdate(sql);
			break;
		case 's':
			parseSelect(sql);
			break;
		}
		return parseInfo;
	}


	private void parseUpdate(String sql) {
		List<String> words = StringUtils.split(sql, " ", true);
		parseInfo.setSql(sql);
		parseInfo.setStatementType(SQLType.UPDATE);
		parseInfo.setTableName(words.get(1));
		
		int idx = words.indexOf("where");
		List<String> params = new ArrayList<String>();
		params.add(null);
		for (int i=0; i<words.size(); ++i){
			String w = words.get(i);
			if (w.indexOf('?')!=-1){
				StringBuilder strBld = new StringBuilder();
				this.parseAWord(strBld, w, 0);
				params.add(strBld.toString());
			}
			if (i<idx && w.indexOf("=")!=-1){
				w = StringUtils.trimSpecialChar(w, new char[]{','});
				String[]strs = parseExpression(w);
				parseInfo.addExp(new Expression(strs[0],AlgRel.EqualTo, strs[2]));
			}
		}
		parseInfo.setParameters(params.toArray(new String[]{}));
		
		
		int whereIdx = sql.indexOf("where");
		if (whereIdx!=-1){
			String whereClause = sql.substring(whereIdx+5);
			String[]s = parseExpression(whereClause);
			parseInfo.addExp(new Expression(s[0], AlgRel.getAlgRel(s[1]), s[2]));
			parseInfo.addCond(s[0]);
		}
		
	}
	
	private void parseDelete(String sql) {
		List<String> words = StringUtils.split(sql, " ", true);
		parseInfo.setSql(sql);
		parseInfo.setStatementType(SQLType.DELETE);
		parseInfo.setTableName(words.get(2));
		
		List<String> params = new ArrayList<String>();
		params.add(null);
		for (String w : words){
			if (w.indexOf('?')!=-1){
				StringBuilder strBld = new StringBuilder();
				this.parseAWord(strBld, w, 0);
				params.add(strBld.toString());
			}
		}
		parseInfo.setParameters(params.toArray(new String[]{}));
		
		int whereIdx = sql.indexOf("where");
		if (whereIdx!=-1){
			String whereClause = sql.substring(whereIdx+5);
			String[]s = parseExpression(whereClause);
			parseInfo.addExp(new Expression(s[0], AlgRel.getAlgRel(s[1]), s[2]));
			parseInfo.addCond(s[0]);
		}
	}
	
	
	protected  String preProcessSQL(String sql){
		int i = findStartOfStatement(sql);
		List<Character> chs = new ArrayList<Character>();
		chs.add(' ');
		for (;i<sql.length(); ++i){
			int s = chs.size();
			char c = sql.charAt(i);
			char l = chs.get(s-1);
			if ((c==' '||c=='='||c=='<'||c=='>'||c=='!'||c=='?'||c==',') && l==' '){
				chs.remove(s-1);
			}
			if (c==' ' && l=='=') continue;
			chs.add(Character.toLowerCase(c));
			if (c==',') chs.add(' ');
		}
		StringBuilder sqlBld = new StringBuilder();
		for (Character ch : chs){
			sqlBld.append(ch.charValue());
		}
		return sqlBld.toString().trim();
	}
	
	
	
	private int parseAWord(StringBuilder sb, String str, int offset) {
		offset = skipSpace(str, offset);
		while (offset < str.length()) {
			char ch = str.charAt(offset);
			if (Character.isLetter(ch) || Character.isDigit(ch)) {
				sb.append(ch);
			} else {
				break;
			}
			++offset;
		}
		return offset;
	}
	
	
	
	private String[] parseExpression(String whereClause){
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		
		int i = parseAWord(sb1, whereClause, 0);
		i = parseRel(sb2, whereClause, i);
		String val = StringUtils.mapEmpty2Null(whereClause.substring(i));
		return new String[]{sb1.toString(), sb2.toString(), val};
	}
	
	private int parseRel(StringBuilder relBld, String str, int offset) {
		offset = skipSpace(str, offset);
		char ch = str.charAt(offset);
		if (Character.isLetter(ch)) {// "is null" or "is not null"
			while (offset < str.length()) {
				ch = str.charAt(offset++);
				if (Character.isLetter(ch))
					relBld.append(ch);
			}
		} else {
			while (offset < str.length()) {// "<=" or "<" or ...
				ch = str.charAt(offset++);
				if (Character.isLetter(ch) || Character.isDigit(ch) || ch == ' ' || ch == '?') {
					--offset;
					break;
				} else {
					relBld.append(ch);
				}
			}
		}
		return offset;
	}
	
	
		
	private void parseSelect(String sql) {
		parseInfo.setStatementType(SQLType.SELECT);
		parseInfo.setSql(sql);
		StringBuilder sb = new StringBuilder();
		String[] s = sql.split("from");
		parseAWord(sb, s[1], 0);
		parseInfo.setTableName(sb.toString());
	}


	private void parseInsert(String sql) {
		// INSERT INTO TABLENAME(C1, C2, C3)VALUES(?, ?, ?);
		parseInfo.setStatementType(SQLType.INSERT);
		int pos = 0;
		pos = skipStrAndWs(sql, "insert", pos);
		pos = skipStrAndWs(sql, "into", pos);
		StringBuilder sb = new StringBuilder();
		parseAWord(sb, sql, pos);
		parseInfo.setTableName(sb.toString());
		
		Pattern pattern = Pattern.compile("(\\([a-zA-Z\\d, ' ?]*\\))", Pattern.MULTILINE | Pattern.UNIX_LINES);
		Matcher matcher = pattern.matcher(sql);
		List<String> colPars = new ArrayList<String>();
		while (matcher.find()) {
			String match = matcher.group();
			colPars.add(StringUtils.trimSpecialChar(match, new char[]{'(', ')', ' '}));
		}
		List<String> cols = StringUtils.split(colPars.get(0), ",", true);
		List<String> vals = StringUtils.split(colPars.get(1), ",", true);
		List<String> params = new ArrayList<String>();
		params.add(null);
		for (int i=0; i<cols.size(); ++i){
			String col = cols.get(i);
			String val = vals.get(i);
			if ("?".equals(val))
				params.add(col);
			parseInfo.addExp(new Expression(col, AlgRel.EqualTo, StringUtils.mapEmpty2Null(val)));
		}
		parseInfo.setParameters(params.toArray(new String[]{}));
	}

	
	private int skipStrAndWs(String str, String skippedStr, int pos) {
		pos = skipSpace(str, pos);
		if (str.startsWith(skippedStr, pos)) {
			int len = skippedStr.length();
			pos = skipSpace(str, pos + len);
		}
		return pos;
	}

	private int skipSpace(String str, int pos) {
		for (int i = pos; i < str.length(); ++pos) {
			if (str.charAt(pos) != ' ' && str.charAt(pos) != '\t') {
				break;
			}
		}
		return pos;
	}

	private int findStartOfStatement(String sql) {
		int statementStartPos = 0;
		if (StringUtils.startsWithIgnoreCaseAndWs(sql, "/*")) {
			statementStartPos = sql.indexOf("*/");
			if (statementStartPos == -1) {
				statementStartPos = 0;
			} else {
				statementStartPos += 2;
			}
		} else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "--")
				|| StringUtils.startsWithIgnoreCaseAndWs(sql, "#")) {
			statementStartPos = sql.indexOf('\n');
			if (statementStartPos == -1) {
				statementStartPos = sql.indexOf('\r');
				if (statementStartPos == -1) {
					statementStartPos = 0;
				}
			}
		}
		return statementStartPos;
	}

	//protected static SqlParser parser;

	public static ParseInfo parse(String sql) throws SQLException {
		SqlParser parser = new SqlParser();
		return parser.parseSql(sql);
	}

	public static void main(String[] args) {
		try {
			//String sql = "INSERT INTO    TABLENAME  ( C1 , C2 , C3 ) VALUES  ( 'ab' , ? , ?)  ";
			//String sql = "delete from TABLENAME where col1=2 "; 
			String sql = "update TABLENAME set col1=?, col2=12  where col1 <> ? ";
			ParseInfo parseInfo = SqlParser.parse(sql);
			
			System.out.println(parseInfo.toString());
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}