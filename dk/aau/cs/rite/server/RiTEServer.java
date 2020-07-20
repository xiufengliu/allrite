/*
 * RiTEServer.java
 *
 * Copyright (c) 2007, Christian Thomsen (chr@cs.aau.dk) and the EIAO Consortium
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
 * Created on January 19, 2007, 2:19 PM
 *
 */

package dk.aau.cs.rite.server;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;

import dk.aau.cs.rite.common.CmdLineParser;

/**
 * The main class for the RiTE memory server. Sets up a server with "memory
 * tables" that hold rows of given types. The server listens on a specified port
 * for a producer (only one producer may be connected at any time) that can
 * insert data, consumers (many consumers can be connected at the same time)
 * that can fetch data, and a ping listener (again, only one can exist at any
 * time) that can be told when data is needed by a consumer.
 */
public class RiTEServer {

	private ServerSocketChannel channel;
	private SharedDataArea dsa;
	public boolean keepRunning;

	/**
	 * Constructor. Corresponds to RiTEServer(5433).
	 */
	

	/**
	 * Constructor.
	 * 
	 * @param port
	 *            The port number for the server to listen on
	 */
	public RiTEServer(int port, String dir, int segmentSize) {
		ExecutorService highExecutor = null;
		ExecutorService lowExecutor = null;
		this.dsa = new SharedDataArea(dir, segmentSize*1024*1024);

		this.keepRunning = true;

		System.out.printf("%s: running on port=%d%s,segmentSize=%dM\n",
				dir == null ? "Mem-based" : "File-based", port,
				dir == null ? "" : ",dataDir=" + dir, segmentSize);
		try {
			new Thread(new ServerInterface(this)).start();
			channel = ServerSocketChannel.open();
			channel.socket().bind(new InetSocketAddress(port));
			highExecutor = PriorityExecutor.getHighExecutor(); // Executors.newCachedThreadPool();
			highExecutor = PriorityExecutor.getLowExecutor(); // Executors.newCachedThreadPool();
			while (keepRunning) {
				highExecutor.execute(new ServerThread(channel.accept(), dsa));

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			/*
			 * if (executorService != null) { executorService.shutdown(); }
			 */
			PriorityExecutor.shutdown();
			this.dsa.clear();
		}
	}

	/**
	 * Shuts down the server, but does not exit before all clients have
	 * disconnected.
	 */
	public void shutdownNow() {
		keepRunning = false;
	}

	public String toString() {
		return this.dsa.toString();
	}

	private static void printUsage() {
		System.err
				.println("Usage: RiTEServer [-p port] [-d /path/to/backup] [-s segmentSize]\ndefault: port=5433, dataDir=null, segmentSize=3M");
	}

	public static void main(String[] args) {
		try {
			CmdLineParser parser = new CmdLineParser();
			CmdLineParser.Option portArg = parser.addIntegerOption('p', "port");
			CmdLineParser.Option dirArg = parser
					.addStringOption('d', "dataDir");
			CmdLineParser.Option segSizeArg = parser.addIntegerOption('s',
					"segmentSize");
			parser.parse(args);

			Integer port = (Integer) parser.getOptionValue(portArg,
					new Integer(5433));
			String dir = (String) parser.getOptionValue(dirArg, null);
			Integer segSize = (Integer) parser.getOptionValue(segSizeArg,3);

			new RiTEServer(port.intValue(), dir, segSize.intValue());
		} catch (CmdLineParser.OptionException e) {
			printUsage();
			System.exit(2);
		}
	}
}