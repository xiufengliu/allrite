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

package dk.aau.cs.rite.server;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.util.HashMap;
import java.util.Map;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.producer.staging.Catalog;
import dk.aau.cs.rite.tuplestore.FileBasedTupleStore;
import dk.aau.cs.rite.tuplestore.MemBasedTupleStore;
import dk.aau.cs.rite.tuplestore.Persistence;
import dk.aau.cs.rite.tuplestore.TupleStore;

public class SharedDataArea {

	protected Map<String, TupleStore> tupleStores = new HashMap<String, TupleStore>();

	protected String dirctory; // TODO:fix me!! hardcode here!!!
	protected PingServer pingServer;
	protected final int segmentSize;

	public SharedDataArea(String dir, int segmentSize) {
		this.dirctory = dir;
		this.segmentSize = segmentSize;
		this.initialize();
	}

	synchronized public TupleStore ensureCatalogAndTupleStore(
			ByteChannel channel) throws RiteException {
		try {
			String tableName = Utils.readString(channel);
			TupleStore tupleStore = tupleStores.get(tableName);
			Catalog catalog = null;
			if (tupleStore == null) {
				catalog = new Catalog(tableName, this.dirctory);
				catalog.readIn(channel);
				if (this.dirctory != null) {
					String tupleStoreDir = String.format("%s%s%s", this.dirctory, File.separator, tableName);
					Utils.mkdir(new File(tupleStoreDir)); // e.g., /data/rite/lineitem
					catalog.backup();
					tupleStore = new FileBasedTupleStore(tupleStoreDir,	catalog, segmentSize);
				} else {
					tupleStore = new MemBasedTupleStore(catalog, segmentSize);
				}
				tupleStores.put(tableName, tupleStore);
			} else {
				catalog = tupleStore.getCatalog();
				catalog.readButNotSave(channel);// We have to re-read it if the
												// tuplestore already exists
			}
			return tupleStore;
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	public TupleStore getTupleStore(String tableName) {
		return tupleStores.get(tableName);
	}

	public PingServer ensurePingServer() {
		if (pingServer == null) {
			pingServer = new PingServer();
		}
		return pingServer;
	}

	public void clear() {
		for (TupleStore tupleStore : tupleStores.values()) {
			tupleStore.close();
		}
	}

	protected void initialize() {
		try {
			if (this.dirctory != null) {
				File dataDir = new File(this.dirctory);
				Utils.mkdir(dataDir);
				File[] listOfFiles = dataDir.listFiles();
				for (int i = 0; i < listOfFiles.length; ++i) {
					File f = listOfFiles[i];
					if (f.isDirectory()) {
						String tableName = f.getName();
						Catalog catalog = new Catalog(tableName, this.dirctory);
						catalog.rollback();
						String tupleStoreDir = String.format("%s%s%s",
								this.dirctory, File.separator, tableName);
						TupleStore tupleStore = new FileBasedTupleStore(
								tupleStoreDir, catalog, segmentSize);
						tupleStore.rollback();
						tupleStores.put(tableName, tupleStore);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		for (TupleStore tupleStore : tupleStores.values()) {
			strBuilder.append(tupleStore).append("\n");
		}
		return strBuilder.toString();
	}

}
