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
package dk.aau.cs.rite.tuplestore;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.logging.Logger;

public class Register {
	Logger log = Logger.getLogger(Register.class.getName());

	String name;
	TreeMap<Integer, Counter> registerCounter = new TreeMap<Integer, Counter>();
	

	 Register(String name){
		this.name = name;
	}
	
	public void put(int rowID, Counter counter) {
		registerCounter.put(rowID, counter);
	}
	
	public Counter get(int rowID){
		return registerCounter.get(rowID);
	}

	public boolean registe(int minRowID, int maxRowID) {
		try {
			NavigableMap<Integer, Counter> registers = registerCounter.subMap(
					minRowID, false, maxRowID, false);
			log.info(String.format("Register %s: min=%d, max=%d, match=%d\n", name, minRowID, maxRowID, registers.size()));
			for (Integer rowID : registers.keySet()) {
				Counter counter = registers.get(rowID);
				counter.inc();
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public void unregiste(int minRowID, int maxRowID) {
		NavigableMap<Integer, Counter> registers = registerCounter.subMap( minRowID, false, maxRowID, false);
		log.info(String.format("UNREGISTER %s: min=%d, max=%d, match=%d\n", name, minRowID, maxRowID, registers.size()));
		for (Integer rowID : registers.keySet()) {
			Counter counter = registers.get(rowID);
			counter.dec();
		}
	}
	
	
}
