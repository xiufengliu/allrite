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

import java.util.ArrayList;
import java.util.List;

import dk.aau.cs.rite.common.Utils;

public class DataGenerator {

	Config config;

	public DataGenerator(Config config) {
		this.config = config;
	}

	public boolean generateData(int size) {
		try {
			List<String> cmds = new ArrayList<String>();
			cmds.add("/usr/bin/killall");
			cmds.add("-9");
			cmds.add("dbgen");
			Utils.execShellCmd(cmds, "/tmp", true);
			cmds.clear();

			cmds.add("" + "/dbgen");
			cmds.add("-f");
			cmds.add("-T");
			cmds.add("L");
			cmds.add("-s");
			cmds.add("0." + size);
			Utils.execShellCmd(cmds, "", true);
			cmds.clear();

			cmds.add("" + "/dbgen");
			cmds.add("-f");
			cmds.add("-U");
			cmds.add("1");
			cmds.add("-s");
			cmds.add("0." + size);
			Utils.execShellCmd(cmds, "", true);
		} catch (Exception e) {
			System.out.printf("Failed to generate the data 0.%d GB\n", size);
			return false;
		}
		return true;
	}
}
