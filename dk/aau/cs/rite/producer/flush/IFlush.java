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


/**
 * Used to tell if a flush (transferral of rows to the RiTEServer) should be
 * done now.
 * 
 * @author chr
 */
public interface IFlush {
	/**
	 * Returns whether it is time to flush. It is up to the implementation of
	 * this interface how to decide this.
	 */
	boolean flushNow();

	/**
	 * Used to inform the FlushScheduler that a full flush has been done. This
	 * method is called from <code>RiTEProducerConnectionProvider</code>
	 * immediately after a full flush (i.e., a transferral of data for all
	 * locally cached tables) has been performed. It is up to the
	 * implementatation of this interface how to use this information.
	 */

	long getFlushTime(String name);

	
	void done();

}
