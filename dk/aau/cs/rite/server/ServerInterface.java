/*
 * ServerInterface.java
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
 * Created on January 23, 2007, 1:57 PM
 *
 */

package dk.aau.cs.rite.server;

import java.io.Console;

/**
 * A text-based user interface to the server. The class is a thread that reads 
 * input from the applications console if such a console exists. If a console 
 * does not exist, the <code>run</code> method will just exit.
 *
 * @author chr
 */
public class ServerInterface implements Runnable {
    protected RiTEServer server;
    protected Console con;
    
    /** Creates a new instance of ServerInterface */
    public ServerInterface(RiTEServer server) {
        this.server = server;
    }
    
    @Override
    public void run() {
        try {
            con = System.console();
            if(con == null) {
                // There is no console. We stop the interface now
                return;
            }
            
            con.printf("RiTE Server version 0.2.0-alpha-2\n\n");
            con.printf("Type 'stop' to shutdown, 'stop now' to force exit "
                    + "immediately,\nor 'state' to see state info\n\n");
            
            while(!Thread.interrupted()) {
                con.printf("RiTE> ");
                String cmd = con.readLine();
                if(cmd == null) // CTRL-D
                    break;
                cmd = cmd.trim();
                if(cmd.equals("stop"))
                    // Exits the loop and calls server.shutdown, but the server
                    // will wait for clients to disconnect
                    break;
                else if(cmd.equals("stop now"))
                    System.exit(1);
                else if(cmd.equals("state")) {
                    printState();
                }
                else if(cmd.equals("")) {
                    continue;
                }
                else {
                    con.printf("Unknown command: %s\n", cmd);
                }
            }
        } finally {
            if(con != null)
                server.shutdownNow();
        }
    }
    
    /** Calls <code>printf</code> with the given arguments on the console
     * object if it exists, and otherwise on <code>System.err</code>*/
    public synchronized void printInfo(String info, Object... args) {
        // Note this method may leave the console messy. If the user is typing
        // something in the interface and another thread prints some info out,
        // then the texts will be scrambled.
        
        if(con != null) {
            con.printf("\n");
            con.printf(info, args);
            con.printf("\nRiTE> ");
        } else
            System.err.printf("\n" + info, args);
    }
    
    /** Prints state information about the server.*/
    public void printState() {
    	printInfo(server.toString());
    }
    
   
    /** Transforms byte sizes to sizes readable for humans (e.g. 1024 = 1KB)*/
    private String humanSize(long size) {
        final int GB = 1024 * 1024 * 1024;
        final int MB = 1024 * 1024;
        final int KB = 1024;
        String unit;
        int divisor = 0;
        if(size >= GB) {
            if(size % GB == 0)
                return size / GB + "GB";
            else {
                unit = "GB";
                divisor = GB;
            }
        }
        else if(size >= 1024 * 1024) {
            if(size % MB == 0)
                return size / MB + "MB";
            else {
                unit = "MB";
                divisor = MB;
            }
        }
        else if(size >= 1024) {
            if(size % KB == 0)
                return size / KB + "KB";
            else {
                unit = "KB";
                divisor = KB;
            }
        }
        else 
            return size + " B";
        
        return String.format("%.2f%s", size / (float)divisor, unit);
    }

}
