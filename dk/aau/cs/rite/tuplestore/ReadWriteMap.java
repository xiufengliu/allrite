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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.RiteException;

public class ReadWriteMap<K, V> implements  Observer {
	// Maintain a readable and a writable TreeMap, one for read by many conumsers, 
	//the other for writing data by producer. Before a flush, the data from rMap is shadow copied to the wMap. 
	//When the flush is finised, the two maps exchange by swap. 
	
	Logger log = Logger.getLogger(ReadWriteMap.class.getName());
	private final ReentrantReadWriteLock l0 = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock l1 = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock l2 = new ReentrantReadWriteLock();
	private final Lock r = l0.readLock();
	private final Lock w = l0.writeLock();
	
	private final Lock rMap_r = l1.readLock();
	private final Lock rMap_w = l1.writeLock();
	
	private final Lock wMap_r = l2.readLock();
	private final Lock wMap_w = l2.writeLock();

	NavigableMap<K, V> rMap;
	NavigableMap<K, V> wMap;


	public ReadWriteMap() {
		this.wMap = new TreeMap<K, V>();
		this.rMap = new TreeMap<K, V>();
	}
	

	public NavigableMap<K, V> getWritableMap(){
		return this.wMap;
	}
	
	public NavigableMap<K, V> getReadableMap(){
		return this.rMap;
	}
	

	
	public Comparator comparator() {
		r.lock();
		try {
			return rMap.comparator();
		} finally {
			r.unlock();
		}
	}

	
	public K firstKey() {
		r.lock();
		try {
			return rMap.firstKey();
		} finally {
			r.unlock();
		}
	}

	
	public K lastKey() {
		r.lock();
		try {
			return rMap.lastKey();
		} finally {
			r.unlock();
		}
	}

	
	public Set<K> keySet() {
		r.lock();
		try {
			return rMap.keySet();
		} finally {
			r.unlock();
		}
	}

	
	public Collection<V> values() {
		r.lock();
		try {
			return rMap.values();
		} finally {
			r.unlock();
		}
	}

	
	public Set<Entry<K, V>> entrySet() {
		r.lock();
		try {
			return rMap.entrySet();
		} finally {
			r.unlock();
		}
	}

	
	public int size() {
		r.lock();
		try {
			return rMap.size();
		} finally {
			r.unlock();
		}
	}

	
	public boolean isEmpty() {
		r.lock();
		try {
			return rMap.isEmpty();
		} finally {
			r.unlock();
		}
	}

	
	public boolean containsKey(Object key) {
		r.lock();
		try {
			return rMap.containsKey(key);
		} finally {
			r.unlock();
		}
	}

	
	public boolean containsValue(Object value) {
		r.lock();
		try {
			return rMap.containsValue(value);
		} finally {
			r.unlock();
		}
	}

	
	public V get(Object key) {
		r.lock();
		try {
			return rMap.get(key);
		} finally {
			r.unlock();
		}
	}

	public V getFromRMap(Object key) {
		rMap_r.lock();
		try {
			return rMap.get(key);
		} finally {
		rMap_r.unlock();
		}
	}
	
	public V putInRMap(K key, V value) {
		rMap_w.lock();
		try {
			return rMap.put(key, value);
		} finally {
			rMap_w.unlock();
		}
	}
	
	
	public V getFromWMap(Object key) {
		wMap_r.lock();
		try {
			return wMap.get(key);
		} finally {
		wMap_r.unlock();
		}
	}
	
	public V putInWMap(K key, V value) {
		wMap_w.lock();
		try {
			return wMap.put(key, value);
		} finally {
			wMap_w.unlock();
		}
	}
	
	
	public V put(K key, V value) {
		return wMap.put(key, value);
	}

	
	public V remove(Object key) {
		return wMap.remove(key);
	}

	
	public void putAll(Map m) {
		wMap.putAll(m);
	}

	
	public void clear() {
		w.lock();
		try {
			wMap.clear();
			rMap.clear();
		} finally {
			w.unlock();
		}
	}

	
	public Entry<K, V> lowerEntry(K key) {
		r.lock();
		try {
			return rMap.lowerEntry(key);
		} finally {
			r.unlock();
		}
	}

	
	public K lowerKey(K key) {
		r.lock();
		try {
			return rMap.lowerKey(key);
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> floorEntry(K key) {
		r.lock();
		try {
			return rMap.floorEntry(key);
		} finally {
			r.unlock();
		}
	}

	
	public K floorKey(K key) {
		r.lock();
		try {
			return rMap.floorKey(key);
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> ceilingEntry(K key) {
		r.lock();
		try {
			return rMap.ceilingEntry(key);
		} finally {
			r.unlock();
		}
	}

	
	public K ceilingKey(K key) {
		r.lock();
		try {
			return rMap.ceilingKey(key);
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> higherEntry(K key) {
		r.lock();
		try {
			return rMap.higherEntry(key);
		} finally {
			r.unlock();
		}
	}

	
	public K higherKey(K key) {
		r.lock();
		try {
			return rMap.higherKey(key);
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> firstEntry() {
		r.lock();
		try {
			return rMap.firstEntry();
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> lastEntry() {
		r.lock();
		try {
			return rMap.lastEntry();
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> pollFirstEntry() {
		r.lock();
		try {
			return rMap.pollFirstEntry();
		} finally {
			r.unlock();
		}
	}

	
	public Entry<K, V> pollLastEntry() {
		r.lock();
		try {
			return rMap.pollLastEntry();
		} finally {
			r.unlock();
		}
	}

	
	public NavigableMap<K, V> descendingMap() {
		r.lock();
		try {
			return rMap.descendingMap();
		} finally {
			r.unlock();
		}
	}

	
	public NavigableSet<K> navigableKeySet() {
		r.lock();
		try {
			return rMap.navigableKeySet();
		} finally {
			r.unlock();
		}
	}

	
	public NavigableSet<K> descendingKeySet() {
		r.lock();
		try {
			return rMap.descendingKeySet();
		} finally {
			r.unlock();
		}
	}

	
	public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
			boolean toInclusive) {
		r.lock();
		try {
			NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll(rMap.subMap(fromKey, fromInclusive, toKey, toInclusive));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		r.lock();
		try {
			 NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll(rMap.headMap(toKey, inclusive));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		r.lock();
		try {
			 NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll(rMap.tailMap(fromKey, inclusive));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		r.lock();
		try {
			 NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll( rMap.subMap(fromKey, toKey));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	public SortedMap<K, V> headMap(K toKey) {
		r.lock();
		try {
			 NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll(rMap.headMap(toKey));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	public SortedMap<K, V> tailMap(K fromKey) {
		r.lock();
		try {
			 NavigableMap<K, V> copy = new TreeMap<K, V>();
			copy.putAll( rMap.tailMap(fromKey));
			return copy;
		} finally {
			r.unlock();
		}
	}

	
	
	
	// --------------- Extension --------------------------

	
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
		for (K k : rMap.keySet()) {
			wMap.put(k, rMap.get(k));
		}
	}

	private void swapWriteToRead() {
		w.lock();
		try {
			NavigableMap<K, V> tmp = rMap;
			rMap = wMap;
			wMap = tmp;
		} finally {
			w.unlock();
		}
	}
}
