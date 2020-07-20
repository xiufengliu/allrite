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
package dk.aau.cs.rite.test;

import java.util.LinkedList;
import java.util.List;

public class ArQueue {

	List<Integer> queue = new LinkedList<Integer>();

	synchronized public boolean isEmpty() {
		if (queue.isEmpty()) {
			try {
				System.out.println("waitting...");
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	synchronized public void add(Integer e) {
		queue.add(e);
		notifyAll();
	}

	synchronized public Integer poll() {
		return queue.remove(0);
	}

	synchronized public int delLast() {
		int n = queue.size();
		if (n > 0) {
			queue.remove(n - 1);
			return 1;
		} else {
			return 0;
		}
	}

	synchronized public int size() {
		return queue.size();
	}

	public static void main(String[] args) {
		ArQueue q = new ArQueue();
		Thread consumer = new Consumer(q);
		consumer.start();

		Thread producer =new Producer(q);
		producer.start();

	}
}

class Producer extends Thread {
	ArQueue q;

	Producer(ArQueue q) {
		this.q = q;
	}

	@Override
	public void run() {
		int i = 0;

		while (true) {
			q.add(i++);
			System.out.println("size="+q.size());
			try {
				sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

class Consumer extends Thread {
	ArQueue q;

	Consumer(ArQueue q) {
		this.q = q;
	}

	public void run() {
		while (true) {
			if (!q.isEmpty()) {
				System.out.println(q.poll());
			}
		}
	}
}
