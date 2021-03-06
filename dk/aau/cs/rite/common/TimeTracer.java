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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class TimeTracer {

	public class TimeLog {
		long start, end;

		public long diff() {
			return end - start;
		}

		@Override
		public String toString() {
			// return start + " - " + end;
			return new StringBuilder(String.valueOf((end - start) / (1000000000.0f))).append(" sec").toString();
		}
	}

	private HashMap<String, LinkedList<TimeLog>> traceLog;

	private static TimeTracer tracer;
	private boolean timing = false;

	private TimeTracer(boolean timing) {
		this.timing = timing;
		traceLog = new HashMap<String, LinkedList<TimeLog>>();
	}

	public static TimeTracer thiz() {
		return TimeTracer.thiz(true);
	}

	public static TimeTracer thiz(boolean timing) {
		if (tracer == null) {
			tracer = new TimeTracer(timing);
		}
		return tracer;
	}

	public void start(String name) {
		if (!timing)
			return;
		LinkedList<TimeLog> list = traceLog.get(name);
		if (list == null) {
			list = new LinkedList<TimeLog>();
			traceLog.put(name, list);
		}
		TimeLog t = new TimeLog();
		t.start = System.nanoTime();
		list.add(t);
	}

	public void start() {
		this.start(null);
	}

	public void end(String name, String message) {
		if (!timing)
			return;
		TimeLog timeLog = traceLog.get(name).peekLast();
		timeLog.end = System.nanoTime();
		if (message != null)
			System.out.println(new StringBuilder(message).append(": ").append(timeLog).toString());
		else
			System.out.println(timeLog);
	}

	public void end() {
		this.end(null, null);
	}

	public List<TimeLog> list(String name) {
		return timing ? traceLog.get(name) : null;
	}

	public void reset() {
		if (timing)
			traceLog = new HashMap<String, LinkedList<TimeLog>>();
	}

	@Override
	public String toString() {
		if (timing && !traceLog.isEmpty()) {
			StringBuilder strBuilder = new StringBuilder();
			Iterator<Entry<String, LinkedList<TimeLog>>> itr = traceLog.entrySet().iterator();
			while (itr.hasNext()) {
				Entry<String, LinkedList<TimeLog>> entry = itr.next();
				String key = entry.getKey();
				if (key != null) {
					strBuilder.append(key).append(": ");
				}
				List<TimeLog> timeLogList = entry.getValue();
				for (TimeLog timeLog : timeLogList) {
					strBuilder.append(timeLog.toString()).append("\n");
				}
			}
			return strBuilder.toString();
		}
		return "";
	}

	public void print() {
		System.out.println(this);
	}
}