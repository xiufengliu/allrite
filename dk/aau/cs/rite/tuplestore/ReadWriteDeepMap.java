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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class ReadWriteDeepMap implements Map, Observer, Persistence {

	Logger log = Logger.getLogger(ReadWriteDeepMap.class.getName());
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	
	private final Lock r = rwl.readLock();
	private final Lock w = rwl.writeLock();

	Map<Object, List<Object>> rMap = new HashMap<Object, List<Object>>();
	Map<Object, List<Object>> wMap = new HashMap<Object, List<Object>>();
	
	
	
	
	
	@Override
	public List<Object> get(Object key) {
		r.lock();
		try {
			return rMap.get(key);
		} finally {
			r.unlock();
		}
	}


	public Map<Object, List<Object>> getWritableMap(){
		return wMap;
	}
	
	@Override
	public List<Object> put(Object key, Object value) {
		List<Object> list = wMap.get(key);
		if (list == null) {
			list = new ArrayList<Object>();
		}
		list.add(value);
		return wMap.put(key, list);
	}


	
	
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return rMap.size();
	}



	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean containsKey(Object key) {
		// TODO Auto-generated method stub
		return false;
	}






	@Override
	public boolean containsValue(Object value) {
		return false;
	}



	@Override
	public List<Object> remove(Object key) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void putAll(Map m) {
		// TODO Auto-generated method stub
		
	}






	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}






	@Override
	public Set keySet() {
		// TODO Auto-generated method stub
		return null;
	}






	@Override
	public Collection values() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Set entrySet() {
		// TODO Auto-generated method stub
		return null;
	}
	
	// --------------- Extension --------------------------

	@Override
	public void update(Observable o, Object arg) {
		switch ((FlushEvent.event) arg) {
		case FLUSH_DATE:
			copyReadToWrite();
			break;
		case FLUSH_SUCCESS:
			swapWriteToRead();
			break;
		case FLUSH_FAIL:
			clearWrite();
			break;
		default:
			;
		}
	}

	private void clearWrite(){
		wMap.clear();
	}
	
	private void copyReadToWrite() {
		wMap.clear();
		for (Object k : rMap.keySet()) {
			wMap.put(k, new ArrayList<Object>(rMap.get(k)));
		}
	}

	private void swapWriteToRead() {
		w.lock();
		try {
			Map<Object, List<Object>> tmp = rMap;
			rMap = wMap;
			wMap = tmp;
		} finally {
			w.unlock();
		}
	}


	@Override
	public void backup(String path) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void recover() {
		// TODO Auto-generated method stub
		
	}
}
