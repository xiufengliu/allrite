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

import java.util.List;

public class Update extends Delete {

	private List<Expression> updateExps;
	
	public Update(List<Expression> updateExps, Expression whereExp) {
		super(whereExp);
		this.updateExps = updateExps;
	}

	@Override
	public Object[] process(Object[] row) {
		if (row != null &&  (whereExp==null || whereExp.eval(row))) {// When whereExp==null, it means that the SQL is "update tablename set col1=?, ...", no where clause
			for (Expression exp : updateExps) {
				int i = exp.getCol();
				row[i] = exp.getVal();
			}
		}
		return row;
	}
}
