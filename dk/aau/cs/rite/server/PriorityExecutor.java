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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class PriorityExecutor {


	static Logger logger = Logger.getLogger(PriorityExecutor.class.getName());



	private static final int cpuNb = Runtime.getRuntime().availableProcessors() + 1;

	private static volatile ExecutorService highExecutor;

	private static volatile ExecutorService lowExecutor;



	private PriorityExecutor() {
	}



	public static ExecutorService getHighExecutor() {
		if (highExecutor == null) {
			synchronized (PriorityExecutor.class) {
				if (highExecutor == null) {
					highExecutor = Executors.newFixedThreadPool(cpuNb + 1,
							new HighFactory());
				}
			}
		}

		return highExecutor;
	}


	public static ExecutorService getLowExecutor() {
		if (lowExecutor == null) {
			synchronized (PriorityExecutor.class) {
				if (lowExecutor == null) {
					lowExecutor = Executors.newFixedThreadPool(cpuNb + 1,
							new LowFactory());
				}
			}
		}

		return lowExecutor;
	}


	public static int getNumberOfCpus() {
		return cpuNb;
	}

	public static void shutdown() {
		if (lowExecutor != null) {
			logger.info("Shutting down low executors");
			shutdownAndAwaitTermination(lowExecutor);
		}

		if (highExecutor != null) {
			logger.info("Shutting down high executors");
			shutdownAndAwaitTermination(highExecutor);
		}
	}


	public static boolean useParallelism() {
		return true;
	}


	private static void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted

		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
				// Cancel currently executing tasks
				pool.shutdownNow();

				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
					logger.warning("Pool did not terminate");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}


	private abstract static class Factory implements ThreadFactory {
		protected final ThreadGroup group;

		Factory() {
			SecurityManager s = System.getSecurityManager();
			group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
					.getThreadGroup();
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r, getThreadName(), 0);

			if (t.isDaemon()) {
				t.setDaemon(false);
			}

			return t;
		}

		protected abstract String getThreadName();
	}



	private static class HighFactory extends Factory {
		private final AtomicInteger highThreadNumber = new AtomicInteger(1);

		@Override
		public Thread newThread(Runnable r) {
			Thread t = super.newThread(r);

			if (t.getPriority() != Thread.NORM_PRIORITY) {
				t.setPriority(Thread.NORM_PRIORITY);
			}

			return t;
		}

		@Override
		protected String getThreadName() {
			return "high-thread-" + highThreadNumber.getAndIncrement();
		}
	}


	private static class LowFactory extends Factory {
		private final AtomicInteger lowThreadNumber = new AtomicInteger(1);

		@Override
		public Thread newThread(Runnable r) {
			Thread t = super.newThread(r);

			if (t.getPriority() != Thread.MIN_PRIORITY) {
				t.setPriority(Thread.MIN_PRIORITY);
			}

			return t;
		}

		@Override
		protected String getThreadName() {
			return "low-thread-" + lowThreadNumber.getAndIncrement();
		}
	}
}
