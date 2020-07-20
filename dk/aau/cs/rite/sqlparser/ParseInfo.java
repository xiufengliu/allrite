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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dk.aau.cs.rite.tuplestore.ud.Expression;

public class ParseInfo {

	SQLType statementType;
	String sql;
	String tableName;

	String[] params;
	Object[] paramValues;
	List <Expression> vhs = new ArrayList<Expression>(); // Holds all the col=val pairs;
	Set<String> conds = new HashSet<String>(); // Holds the columns in where clause;
	
	
	public SQLType getStatementType() {
		return statementType;
	}

	public void setStatementType(SQLType statementType) {
		this.statementType = statementType;
	}

	public String[] getParameters() {
		return params;
	}

	public void setParameters(String[] parameters) {
		this.params = parameters;

	}

	public void setParamValues(Object[] paramValues) {
		this.paramValues = paramValues;
	}

	public Object[] getParamValues() {
		return this.paramValues;
	}

	public void addExp(Expression exp){
		this.vhs.add(exp);
	}
	
	public List<Expression> getExps(){
		return this.vhs;
	}
	
	public void addCond(String col){
		this.conds.add(col);
	}
	
	public Set<String>getConds(){
		return this.conds;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getSql() {
		return sql;
	}

	public String toString() {
		StringBuilder strBld = new StringBuilder("sql=").append(sql);
		strBld.append("\ntableName=").append(tableName).append("\n");
		strBld.append("statementType=").append(statementType).append("\n");
		strBld.append("parameters=(");
		if (params != null) {
			for (String par : params) {
				strBld.append(par).append(",");
			}
		}
		strBld.append(")\n");
		strBld.append("allExp=(\n");
		for (Expression exp : vhs){
			strBld.append(exp.toString()).append("\n");
		}
		strBld.append(")\n");
		strBld.append("WhereCols=(");
		for(String col: conds){
			strBld.append(col).append(",");
		}
		strBld.append(")\n");
		return strBld.toString();
	}
}
