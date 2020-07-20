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
package dk.aau.cs.rite.tuplestore;

public 	class Counter {
	int register = 0;
	boolean materialized = false;

	
	public Counter(){}
	
	public Counter(int register, boolean isMaterialized){
		this.register = register;
		this.materialized = isMaterialized;
	}
	
	synchronized public void inc() {
		++register;
	}

	synchronized public void dec() {
		if (register > 0)
			--register;
	}

	synchronized public void markMaterialized() {
		this.materialized = true;
	}

	synchronized public boolean isMaterialized() {
		return this.materialized;
	}

	synchronized public boolean canBeDeleted() {
		return materialized && register == 0;
	}
}
