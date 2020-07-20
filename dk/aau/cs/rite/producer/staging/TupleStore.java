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

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.sqlparser.ParseInfo;

/**
 * A RowArchive is a RowTransferrer that holds its rows in a temporary file.
 * Concretely, it is used for committed rows that have not been transferred to
 * the memory server yet.
 */
public interface TupleStore {

	void insert(ParseInfo row) throws RiteException;

	void updateAndDelete(ParseInfo paramInfo)	throws RiteException;

	void rollback() throws RiteException;

	boolean ensureAccuracy(long age);

	void materialize() throws RiteException;

	void commit(boolean materialize) throws RiteException;

	void close() throws RiteException;
	
	IHolder getRowsHolder();
	UDsHolder getUDsHolder();

	Catalog getCatalog();

}
