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


package dk.aau.cs.rite.producer.flush;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A FlushScheduler that says that it is time to do a full flush when either
 * A) the CPU load is less than or equal to a user-defined value or 
 * B) a user-defined amount of time has passed since the last full flush. 
 * The implementation depends on that the CPU load can be read from the file
 * /proc/loadavg (as is the case for modern Linux versions).
 */
public class LinuxLoadAwareFlush implements IFlush {
    
    private volatile long lastFlush;
    private int cpuLoad;
    private int timeLimit;
    private RandomAccessFile loadAvgs;
    /** Creates a new instance of LinuxLoadAwareFlushScheduler.
     A flush is scheduled when either the CPU load is equal to or
     below <code>cpuLoad</code> or <code>seconds</code> seconds have
     passed since last flush was done.
     *@param cpuLoad Determines whhen to flush. A flush is done when the CPU 
     * load is less than or equal to the given value. The number is in percent,
     * but note that values can be bigger than 100%.
     */
    public LinuxLoadAwareFlush(int cpuLoad, int seconds) {
        this.cpuLoad = cpuLoad;
        timeLimit = seconds * 1000; // get it in milliseconds
        lastFlush = System.currentTimeMillis();
        try {
            loadAvgs = new RandomAccessFile("/proc/loadavg", "r");
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void done() {
        lastFlush = System.currentTimeMillis(); // An approximation...
    }

    @Override
    public boolean flushNow() {
        
        if(System.currentTimeMillis() - lastFlush > timeLimit ||
                getSystemLoad() <= cpuLoad) 
            return true;
        else 
            return false;
    }
    
    public int getSystemLoad() {
        String line = null;
        try {
            loadAvgs.seek(0);
            line = loadAvgs.readLine();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        
        // Read the first float on the line and convert it to an int percentage
        int curLoad = (int)(100 * Float.parseFloat(line.trim().split("\\s")[0]));
        //System.out.printf("load: %d, %d\n", System.currentTimeMillis(), curLoad); // FIXME TEST
        return curLoad;
    }

	@Override
	public long getFlushTime(String name) {
		return Long.MAX_VALUE;
	}
}
