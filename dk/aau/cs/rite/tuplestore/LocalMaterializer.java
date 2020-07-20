package dk.aau.cs.rite.tuplestore;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.tuplestore.ud.UDStore;

public class LocalMaterializer {

	private String tmpDir, finalizedDir;

	// tmp=/path/to/backup/order/tmp/
	// finalized = /path/to/backup/order/finalized

	public LocalMaterializer(String tupleStoreDir) throws RiteException {
		this.tmpDir = String.format("%s%s%s%s", tupleStoreDir, File.separator,
				"tmp", File.separator);
		this.finalizedDir = String.format("%s%s%s%s", tupleStoreDir,
				File.separator, "finalized", File.separator);

		try {
			Utils.mkdir(new File(this.tmpDir));
			Utils.mkdir(new File(this.finalizedDir));
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	public void materializeTimeIndex(ReadWriteMap<Long, Integer> timeIndex)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.tmpDir, "timeindex");
			NavigableMap<Long, Integer> wMap = timeIndex.getWritableMap();
			if (!wMap.isEmpty()) {
				File f = new File(filePath);
				RandomAccessFile raf = new RandomAccessFile(f, "rw");
				Set<Entry<Long, Integer>> entries = wMap.entrySet();
				Iterator<Entry<Long, Integer>> itr = entries.iterator();
				while (itr.hasNext()) {
					Entry<Long, Integer> entry = itr.next();
					long key = entry.getKey().longValue();
					int value = entry.getValue().intValue();
					raf.writeLong(key);
					raf.writeInt(value);
				}
				raf.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void rollbackTimeIndex(ReadWriteMap<Long, Integer> timeIndex)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.finalizedDir,
					"timeindex");

			NavigableMap<Long, Integer> rMap = timeIndex.getReadableMap();
			RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");

			long len = raf.length();
			int cnt = 0;
			while (cnt < len) {
				long time = raf.readLong();
				int rowID = raf.readInt();
				rMap.put(time, rowID);
				cnt += 8+4;
			}
			raf.close();
		} catch (Exception e) {
			throw new RiteException(e);
		}
		
	}

	public void materializeRegCounter(
			ReadWriteMap<Integer, Counter> registerCounter)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.tmpDir, "registerCounter");
			NavigableMap<Integer, Counter> wMap = registerCounter.getWritableMap();
			if (!wMap.isEmpty()) {
				File f = new File(filePath);
				RandomAccessFile raf = new RandomAccessFile(f, "rw");
				//BufferedWriter fa = new BufferedWriter(new FileWriter(raf));
				
				//FileOutputStream fos = new FileOutputStream(raf);

				Set<Entry<Integer, Counter>> entries = wMap.entrySet();
				Iterator<Entry<Integer, Counter>> itr = entries.iterator();
				while (itr.hasNext()) {
					Entry<Integer, Counter> entry = itr.next();
					int key = entry.getKey().intValue();
					Counter counter = entry.getValue();
					if (!counter.canBeDeleted()) {
						int value = counter.isMaterialized() ? 1 : 0;
						//raf.write(Utils.intToByteArray(key));
						//raf.write(Utils.intToByteArray(value));
						raf.writeInt(key);
						raf.writeInt(value);
					}
				}
				//raf.flush();
				raf.close();
				//fos.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void rollbackRegCounter(
			ReadWriteMap<Integer, Counter> registerCounter)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.finalizedDir,"registerCounter");
			File file = new File(filePath);
			if (file.exists()){
				NavigableMap<Integer, Counter> rMap = registerCounter.getReadableMap();
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				long len = raf.length();
				int cnt = 0;
				while (cnt < len) {
					int rowID = raf.readInt();
					int flag = raf.readInt();
					rMap.put(rowID, new Counter(0, flag == 1));
					cnt += 4 * 2;
				}
				raf.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}

	}

	public void materializeRowIndex(ReadWriteMap<Integer, Integer> rowIndex)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.tmpDir, "rowIndex");
			NavigableMap<Integer, Integer> wMap = rowIndex.getWritableMap();
			if (!wMap.isEmpty()) {
				File f = new File(filePath);
				RandomAccessFile raf = new RandomAccessFile(f, "rw");
				Set<Entry<Integer, Integer>> entries = wMap.entrySet();
				Iterator<Entry<Integer, Integer>> itr = entries.iterator();
				while (itr.hasNext()) {
					Entry<Integer, Integer> entry = itr.next();
					int key = entry.getKey().intValue();
					int value = entry.getValue().intValue();
					raf.writeInt(key);
					raf.writeInt(value);
				}
				raf.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void rollbackRowIndex(ReadWriteMap<Integer, Integer> rowIndex)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.finalizedDir,"rowIndex");

			NavigableMap<Integer, Integer> rMap = rowIndex.getReadableMap();
			RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");

			long len = raf.length();
			int cnt = 0;
			while (cnt < len) {
				int rowID = raf.readInt();
				int segID = raf.readInt();
				rMap.put(rowID, segID);
				cnt += 4 * 2;
			}
			raf.close();
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void materializeSegCache(
			ReadWriteMap<Integer, SoftReference<Segment>> segCache)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.tmpDir, "segCache");
			NavigableMap<Integer, SoftReference<Segment>> wMap = segCache
					.getWritableMap();
			if (!wMap.isEmpty()) {
				File f = new File(filePath);
				RandomAccessFile raf = new RandomAccessFile(f, "rw");
				Set<Entry<Integer, SoftReference<Segment>>> entries = wMap
						.entrySet();
				Iterator<Entry<Integer, SoftReference<Segment>>> itr = entries
						.iterator();
				while (itr.hasNext()) {
					Entry<Integer, SoftReference<Segment>> entry = itr.next();
					int key = entry.getKey().intValue();
					raf.writeInt(key);
				}
				raf.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void rollbackSegCache(
			ReadWriteMap<Integer, SoftReference<Segment>> segCache)
			throws RiteException {
		try {
			String filePath = String.format("%s%s", this.finalizedDir,"segCache");

			NavigableMap<Integer, SoftReference<Segment>> rMap = segCache.getReadableMap();
			RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");

			long len = raf.length();
			int cnt = 0;
			while (cnt < len) {
				int segID = raf.readInt();
				rMap.put(segID, new SoftReference<Segment>(null));
				cnt += 4;
			}
			raf.close();
		} catch (Exception e) {
			throw new RiteException(e);
		}

	}

	public void materializeOthers(int... args) throws RiteException {
		try {
			String filePath = String.format("%s%s", this.tmpDir, "misc");
			if (args.length > 0) {
				File f = new File(filePath);
				RandomAccessFile raf = new RandomAccessFile(f, "rw");
				for (int arg : args) {
					raf.writeInt(arg);
				}
				raf.close();
			}
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}
	
	public int []rollbackOthers() throws RiteException {
		try {
			int [] others = new int[4];
			String filePath = String.format("%s%s", this.finalizedDir,"misc");
			
			RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");
			for (int i=0; i<others.length; ++i){
				others[i] = raf.readInt();
			}
			raf.close();
			return others;
		} catch (Exception e) {
			throw new RiteException(e);
		}
	}

	public void finalize() throws RiteException {
		try {
			Utils.deleteDirectory(new File(this.finalizedDir));
			new File(this.tmpDir).renameTo(new File(this.finalizedDir));
			Utils.mkdir(new File(this.tmpDir));
		} catch (IOException e) {
			throw new RiteException(e);
		}
	}

	public void materializeUDStore(UDStore udStore) throws RiteException {
		String filePath = String.format("%s%s%s", this.tmpDir, "udStore",
				File.separator);
	}

	public void rollbackUDStore(UDStore udStore) {
		// TODO Auto-generated method stub

	}

}
