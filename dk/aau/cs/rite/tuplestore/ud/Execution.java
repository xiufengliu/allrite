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

package dk.aau.cs.rite.tuplestore.ud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dk.aau.cs.rite.sqlparser.SQLType;

public class Execution implements Execute {
	// This object is used to save the scalar values for execution of a prepared statement.
	
	int ID;
	
	SQLType type;
	
	List<Expression> exps = new ArrayList<Expression>(); // all the expressioin (colindex, rel value)
	
	int[] params; // save the colindex for parameters in SQL
	List<Integer> conds = new ArrayList<Integer>(); // save the colindex for cols in Where caluse
	
	String sql;
	
	public Execution(int ID){
		this.ID = ID;
	}
	
	public int getID(){
		return this.ID;
	}
	
	public void setType(SQLType type){
		this.type = type;
	}
	
	public SQLType getType(){
		return this.type;
	}
	
	public void setParams(int[] params){
		this.params = params;
	}
	
	public int[] getParams(){
		return this.params;
	}
	
	public void addCond(int cond){
		conds.add(cond);
	}
	
	public List<Integer>  getConds(){
		return this.conds;
	}
	
	public void addExp(Expression exp){
		this.exps.add(exp);
	}
	
	public List<Expression> getExps(){
		return this.exps;
	}
	
		
	public void setSQL(String sql){
		this.sql = sql;
	}
	
	public String getSQL(){
		return this.sql;
	}
	

	@Override
	public void accept(Visitor visitor) throws IOException {
		visitor.visit(this);
	}
}
