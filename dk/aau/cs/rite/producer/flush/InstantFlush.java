/*
 * InstantFlusher.java
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
 * Created on February 21, 2007, 3:36 PM
 *
 */

package dk.aau.cs.rite.producer.flush;



/**
 * A FlushScheduler that always says that a flush is required. Thus, flushes are
 * not dealyed when InstantFlusher is used as the FlushScheduler.
 * 
 * @author chr
 */
public class InstantFlush implements IFlush {

    @Override
    public boolean flushNow() {
        return true;
    }

	@Override
	public long getFlushTime(String name) {
		return Long.MAX_VALUE;
	} 

    @Override
    public void done() {
        // Do nothing
    }
}
