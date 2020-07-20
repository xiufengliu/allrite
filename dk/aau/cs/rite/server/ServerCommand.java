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

/**
 * 
 * @author chr
 */
public enum ServerCommand {
	BYE, // The polite way of ending a session
	OK, 
	ERR,
	
	TABLE_FUNC_CONNECT,
	TABLE_FUNC_GET_DATA,
	
	PING_CONNECT,
	PING_ENSURE_ACCURACY, // It is used for the producer to check from the mem  server with a time interval to see if the consumer has requested data.

	PROD_CONNECT,
	PROD_SYNC_CATALOG,
	PROD_COMMIT_MATERIALIZE, // When commit
	PROD_COMMIT_FLUSH_DATA,
	PROD_COMMIT_FLUSH_UD,
	PROD_ROLLBACK,
	


	CUST_CONNECT,
	CUST_REGISTER_QUERY,
	CUST_UNREGISTER_QUERY,
	CUST_ENSURE_ACCURACY,  
}
