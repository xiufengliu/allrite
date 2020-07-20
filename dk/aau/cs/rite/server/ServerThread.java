/*
 * ServerThread.java
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
 * Created on January 19, 2007, 2:43 PM
 *
 */
package dk.aau.cs.rite.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.logging.Logger;

import dk.aau.cs.rite.common.RiteException;
import dk.aau.cs.rite.common.TimeTracer;
import dk.aau.cs.rite.common.Utils;
import dk.aau.cs.rite.tuplestore.FlagValues;
import dk.aau.cs.rite.tuplestore.TupleStore;


public class ServerThread implements Runnable {
	Logger log = Logger.getLogger(ServerThread.class.getName());

	private ByteChannel channel;
	private ByteBuffer buffer, dblBuffer;
	private SharedDataArea sda;
	private long reqFreshness;

	private static ServerCommand[] commands = ServerCommand.values();

	/** Creates a new instance of ServerThread */
	public ServerThread(ByteChannel channel, SharedDataArea sda) {
		this.channel = channel;
		this.sda = sda;
		this.buffer = ByteBuffer.allocate(4); // To read one command/argument
		this.dblBuffer = ByteBuffer.allocate(8); // To read a long, or two int.
		this.reqFreshness = 0;
	}

	private final ServerCommand getCommand() throws IOException {
		buffer.clear();
		int read = Utils.read(channel, 4, buffer); // If no data is available in the channel, it will be blocked here.
		if (read == -1)
			return null;
		int val = buffer.getInt(0);
		ServerCommand cmd;

		if (val >= 0 && val < commands.length)
			cmd = commands[val];
		else
			cmd = ServerCommand.BYE;
		return cmd;
	}

	@Override
	public void run() {
		try {
			while (!Thread.interrupted()) {
				ServerCommand cmd = getCommand();
				
				//System.out.println("cmd=" + cmd);
				switch (cmd) {
				case BYE:
					return;
				case TABLE_FUNC_CONNECT:
					break;
				case TABLE_FUNC_GET_DATA:
					System.out.println("cmd=" + cmd);
					readRows();
					continue;
				case PROD_SYNC_CATALOG:
					//System.out.println("cmd=" + cmd);
					ensureTupleStore();
					break;
				case PROD_COMMIT_MATERIALIZE:
					System.out.println("cmd=" + cmd);
					materialize();
					break;
				case PROD_COMMIT_FLUSH_DATA:
					System.out.println("cmd=" + cmd);
					flushRows();
					break;
				case PROD_COMMIT_FLUSH_UD:
					//System.out.println("cmd=" + cmd);
					flushUDs();
					break;
				case PROD_ROLLBACK:
					rollback();
					break;
				case CUST_CONNECT:
					break;
				case PING_CONNECT:
					pingConnect();
					break;
				case PING_ENSURE_ACCURACY:
					pingAccuracy();
					continue; // Does not need to send OK.
				case CUST_REGISTER_QUERY:
					if (!registeRows()){// If fails to register, just continue and doesn't need to send back anything.
						Utils.send(channel, buffer, ServerCommand.ERR);
						continue;
					}
					break;
				case CUST_UNREGISTER_QUERY:
					unregisterRows();
					return;
				default:
					throw new RuntimeException("Unknown command: " + cmd);
				}
				Utils.send(channel, buffer, ServerCommand.OK);
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				Utils.send(channel, buffer, ServerCommand.ERR);
			} catch (IOException e1) {
			}
		} finally {
			Utils.closeQuietly(channel);
			log.info("I was closed!");
		}
	}
	
	private boolean registeRows() throws IOException {
		String tableName = Utils.readString(channel);
		dblBuffer.clear();
		Utils.read(channel, 8, dblBuffer);
		int minRowID = dblBuffer.getInt(0);
		int maxRowID = dblBuffer.getInt(4);
		reqFreshness = Utils.readLong(channel, dblBuffer); // When do the register, we should give the freshness.
		
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		if (tupleStore != null) {
			return tupleStore.register(minRowID, maxRowID);
		} else {
			throw new IOException(tableName + " does not exist!");
		}
	}
	
	private void unregisterRows() throws IOException {
		String tableName = Utils.readString(channel);
		dblBuffer.clear();
		Utils.read(channel, 8, dblBuffer);
		int minRowID = dblBuffer.getInt(0);
		int maxRowID = dblBuffer.getInt(4);
		
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		tupleStore.unregister(minRowID, maxRowID);
	}

	private void readRows() throws IOException { // Read rows by the table functions.
		String tableName = Utils.readString(channel);
		dblBuffer.clear();
        Utils.read(channel, 8, dblBuffer);
        int minRowID = dblBuffer.getInt(0);
        int maxRowID = dblBuffer.getInt(4);       
        long queryStartTime = Utils.readLong(channel, dblBuffer); //read rite.timehandle
        
        TupleStore tupleStore = this.sda.getTupleStore(tableName);
        if(tupleStore == null) {
            buffer.clear();
            buffer.put(FlagValues.END_OF_STREAM);
            buffer.flip();
            channel.write(buffer);
        } else {
        	tupleStore.query(channel, minRowID, maxRowID, queryStartTime, reqFreshness);
        }
    }

	private void pingConnect() throws IOException { // Only used when the producer is using lazy commit.
		String tableName = Utils.readString(channel, buffer);
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		if (tupleStore!=null){
			tupleStore.setPingServer(this.sda.ensurePingServer()); // Create pingServer for every tupleStore.
		}
	}
	
	private void pingAccuracy() throws IOException {
		int numOfTable = Utils.readInt(channel, buffer);
		if (numOfTable > 0) {
			PingServer pingServer = this.sda.ensurePingServer();
			String[] tableNames = new String[numOfTable];
			for (int i = 0; i < numOfTable; ++i) {
				tableNames[i] = Utils.readString(channel, buffer);
				pingServer.update(channel, tableNames[i]);
			}
		}
	}


	private void flushRows() throws IOException, RiteException {
		String tableName = Utils.readString(channel, buffer);
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		tupleStore.readRowsIn(channel); // Write the data from the channel into the tupleStore.
	}
	
	
	private void flushUDs() throws RiteException, IOException {
		String tableName = Utils.readString(channel, buffer);
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		tupleStore.readUDsIn(channel); 
	}
	
	private void materialize() throws RiteException, IOException  {
		String tableName = Utils.readString(channel, buffer);
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		if (tupleStore != null) {
			 tupleStore.materialize();
		}
	}
	
	
	private void ensureTupleStore() throws RiteException, IOException {		
			TupleStore tupleStore = this.sda.ensureCatalogAndTupleStore(channel);// Create tupleStore with catalog if not exist, otherwise not create.
			buffer.clear();
            buffer.putInt(tupleStore.getID());
            buffer.flip();
            channel.write(buffer);
	}


	private void rollback() throws IOException {
		String tableName = Utils.readString(channel, buffer);
		TupleStore tupleStore = this.sda.getTupleStore(tableName);
		
	}
}
