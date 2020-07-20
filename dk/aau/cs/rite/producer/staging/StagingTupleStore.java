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

package dk.aau.cs.rite.producer.staging;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.sqlparser.ParseInfo;
import dk.aau.cs.rite.tuplestore.ud.AlgRel;
import dk.aau.cs.rite.tuplestore.ud.Execution;
import dk.aau.cs.rite.tuplestore.ud.Expression;


public class StagingTupleStore implements TupleStore {
	Catalog catalog;
	ITransfer transfer;
	IHolder rowsHolder;
	UDsHolder udsHolder;
	
	Map<String, Object> tmpMap = new HashMap<String, Object>();

	public StagingTupleStore(ITransfer transfer, Catalog catalog) throws IOException {
		this.catalog = catalog;
		this.transfer = transfer;
		this.rowsHolder = new MemRowsHolder(catalog.getTypeArray()); // Create re-usable rowsHolder.
		this.udsHolder = new UDsHolder(catalog);
	}

	@Override
	synchronized public void insert(ParseInfo row) throws RiteException {
		try {
			tmpMap.clear();
			String[]paramCols = row.getParameters();
			Object[]paramVals = row.getParamValues();
			for (int i = 1; i < paramCols.length; ++i) {
				tmpMap.put(paramCols[i], paramVals[i]);
			}
			List<String> cols = catalog.getColList();
			Object[] vals = new Object[cols.size()+1];
			for (int i = 0; i < cols.size(); ++i) {
				vals[i] = tmpMap.get(cols.get(i));
			}
			vals[cols.size()] = catalog.getNextSeq(); // The row ID.
			rowsHolder.putRow(vals);
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}
	

	@Override
	synchronized public void updateAndDelete(ParseInfo paramInfo) throws RiteException {
		String[] paramCols = paramInfo.getParameters();
		Object[] paramVals = paramInfo.getParamValues();
		List<Expression> exps = paramInfo.getExps();
		Set<String> conds = paramInfo.getConds();
		int[] params = null;
		if (paramCols.length>1)
			params = new int[paramCols.length-1]; // Params is used to saved the column id of X, for example X=?.
		
		Execution scalarValues = new Execution(catalog.getNextSeq());
		scalarValues.setType(paramInfo.getStatementType());
		scalarValues.setSQL(paramInfo.getSql());
		for (Expression exp : exps) {
			String col = exp.getName();
			int idx = catalog.indexOf(col);
			AlgRel rel = exp.getRel();
			Object val = exp.getVal();
			for (int i = 1;  i < paramCols.length; ++i) {
				if (col.equals(paramCols[i])) {
					val = paramVals[i];
					params[i-1] = idx;
					break;
				}
			}
			scalarValues.addExp(new Expression(idx, rel, val));
			if (conds.contains(col)){
				scalarValues.addCond(idx);
			}
		}
		scalarValues.setParams(params);
		udsHolder.add(scalarValues);
	}
	
	
	@Override
	synchronized public void materialize() throws RiteException {
		try {
			this.transfer.materialize(); // Will materialize the committed rows in the archive and in the catalyst.
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	@Override
	synchronized public void close() throws RiteException {
		try {
			if (this.transfer.isConnected()){
				this.transfer.done();
			}
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	@Override
	synchronized public void rollback() throws RiteException {
			try {
			transfer.rollback();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RiteException(e);
		}
	}
	

	@Override
	synchronized public void commit(boolean materialize) throws RiteException {
		try {
			if (this.transfer.isConnected()){
				transfer.transfer(System.currentTimeMillis(), rowsHolder, udsHolder);
				rowsHolder.clear();
				udsHolder.clear();
				if (materialize) {
					this.transfer.materialize();
				}
			}
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	@Override
	public Catalog getCatalog() {
		return catalog;
	}

	
	@Override
	public boolean ensureAccuracy(long age) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public IHolder getRowsHolder() {
		return this.rowsHolder;
	}

	@Override
	public UDsHolder getUDsHolder() {
		return this.udsHolder;
	}
}
