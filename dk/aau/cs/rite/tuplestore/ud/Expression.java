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

import dk.aau.cs.rite.common.Utils;

public class Expression {

	int col;
	String name;
	AlgRel rel;
	Object val;
	
	long commitTime; 
	

	// for example, C1 = 1, c2 > 2, ...

	public Expression() {
		this(-1, null);
	}

	public Expression(int col, Object val) {
		this(col, AlgRel.EqualTo, val);
	}

	public Expression(AlgRel rel, Object val) {
		this(-1, rel, val);
	}

	public Expression(int col, AlgRel rel, Object val) {
		this.col = col;
		this.rel = rel;
		this.val = val;

	}

	public Expression(String name, AlgRel rel, Object val) {
		this.name = name;
		this.rel = rel;
		this.val = val;
	}

	
	public String getName(){
		return this.name;
	}
	
	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	public AlgRel getRel() {
		return rel;
	}

	public void setRel(AlgRel rel) {
		this.rel = rel;
	}

	public Object getVal() {
		return val;
	}

	public void setVal(Object val) {
		this.val = val;
	}

	public boolean eval(Object[] row) {
		Object valueInRow = row[col];
		int result = Utils.compare(valueInRow, val);
		switch (rel) {
		case EqualTo:
			return result == 0;
		case NotEqualTo:
			return result != 0;
		case GreaterThan:
			return result == 1;
		case GreaterOrEqualTo:
			return result == 1 || result == 0;
		case LessThan:
			return result == -1;
		case LessOrEqualto:
			return result == -1 || result == 0;
		case IsNull:
			return val == null;
		case IsNotNull:
			return val != null;
		default:
			return false;
		}
	}

	public String toString() {
		StringBuilder expStr = new StringBuilder().append(String.valueOf(col))
				.append(" ").append(rel.name()).append(" ").append(val);
		return expStr.toString();
	}
}
