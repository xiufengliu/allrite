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
package dk.aau.cs.rite.common;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * A background output stream for a SINGLE thread.
 * 
 * @author Christian Thomsen
 */

public class BackgroundOutputStream extends OutputStream implements Runnable {

	private int bufferSize;
	private OutputStream out;
	private int cnt = 0;
	private byte[] bytes;
	private volatile byte[] bytesToOutput;
	private volatile boolean flag = true;
	private volatile boolean hasData = false;
	private Thread thread;

	public BackgroundOutputStream(OutputStream out, int bufferSize) {
		this.out = out;
		this.bufferSize = bufferSize;
		this.bytes = new byte[bufferSize];
		thread = new Thread(this);
		thread.setPriority(Thread.MAX_PRIORITY);
		thread.start();

	}

	public BackgroundOutputStream(OutputStream out) {
		this(out, 4 * 1024 * 1024);
	}

	@Override
	public void write(int b) {
		bytes[cnt++] = (byte) b;
		if (cnt == bufferSize)
			flush();
	}

	@Override
	public void write(byte[] ba) throws IOException {
		int amount = ba.length;
		int pos = 0;
		while (amount > 0) {
			int step = Math.min(amount, bufferSize - cnt);
			System.arraycopy(ba, pos, bytes, cnt, step);
			amount -= step;
			pos += step;
			cnt += step;
			if (cnt == bufferSize)
				flush();
		}
	}

	@Override
	public void close() throws IOException {
		finish();
		out.close();
	}

	@Override
	public synchronized void flush() {
		if (cnt == 0) {
			notify();
			return; // Nothing to flush anyway...
		}

		while (hasData) {
			// There is a request that has not been served yet...
			// Wait until it has been done!
			try {
				wait();
			} catch (InterruptedException e) {
				// ...
			}
		}

		// Make copy for the background thread
		bytesToOutput = Arrays.copyOf(bytes, cnt);

		// Start from position 0 in the "working copy"
		cnt = 0;

		hasData = true;
		notify();
	}

	private void finish() {
		flag = false;
		flush();

		try {
			thread.join();
		} catch (InterruptedException e) {
			System.err.println("Could not join thread: " + e);
		}
	}

	@Override
	public synchronized void run() {
		while (flag) {
			try {
				while (!hasData) {
					wait();
					if (!flag) {
						// flag is now false which means that
						// we should break out of the outer while
						// loop. But there might be some data to write
						// before we do that.
						if (!hasData) {
							// There is no data - just stop here
							return;
						} else {
							// There is data.
							// Break out of the inner loop such that
							// we write the data. We break out of the
							// outer loop in its next evaluation then.
							break;
						}
					}
				}

				try {
					out.write(bytesToOutput);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				hasData = false;
				notify();
			} catch (InterruptedException e) {
				System.err.println("Thread interrupted");
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Only for testing
		int testSize = 5000000;

		if (args.length == 1)
			testSize = Integer.parseInt(args[0]);

		String data = "abcdefghijklmnopqrstuvwxyz0123456789";
		PrintStream prn;
		long start, end;

		prn = new PrintStream(new FileOutputStream("/tmp/delete.me"));
		start = System.currentTimeMillis();
		for (int i = 0; i < testSize; i++) {
			prn.println(data);
		}
		prn.close();
		end = System.currentTimeMillis();
		System.out.println("Time usage with stream: " + (end - start));

		prn = new PrintStream(new BufferedOutputStream(new FileOutputStream(
				"/tmp/delete.me")));
		start = System.currentTimeMillis();
		for (int i = 0; i < testSize; i++) {
			prn.println(data);
		}
		prn.close();
		end = System.currentTimeMillis();
		System.out.println("Time usage with buffered stream: " + (end - start));
		System.gc();
		System.gc();
		Thread.sleep(1000);

		prn = new PrintStream(new BackgroundOutputStream(new FileOutputStream(
				"/tmp/delete.me")));
		start = System.currentTimeMillis();
		for (int i = 0; i < testSize; i++) {
			prn.println(data);
		}
		prn.close();
		end = System.currentTimeMillis();
		System.out.println("Time usage with background stream: "
				+ (end - start));
		System.gc();
		System.gc();
		Thread.sleep(1000);

		prn = new PrintStream(new BufferedOutputStream(
				new BackgroundOutputStream(new FileOutputStream(
						"/tmp/delete.me"))));
		start = System.currentTimeMillis();
		for (int i = 0; i < testSize; i++) {
			prn.println(data);
		}
		prn.close();
		end = System.currentTimeMillis();
		System.out.println("Time usage with buffered background stream: "
				+ (end - start));

		File file = new File("/tmp/delete.me");
		file.delete();

	}
}
