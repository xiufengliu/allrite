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

import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NOT_NULL;
import static dk.aau.cs.rite.tuplestore.FlagValues.IS_NULL;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import dk.aau.cs.rite.server.ServerCommand;

public class Utils {

	public static final byte UNKNOWN = 0;
	public static final byte NULL = 1;
	public static final byte BOOLEAN = 5; // internal use only
	public static final byte BYTE = 6; // internal use only
	public static final byte INTEGER = 10;
	public static final byte LONG = 15;
	public static final byte FLOAT = 20;
	public static final byte DOUBLE = 25;
	public static final byte BYTEARRAY = 50;
	public static final byte CHARARRAY = 55;
	public static final byte ERROR = -1;

	final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
			'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
			'z' };

	public static int[] toIntArray(List<Integer> integers) {
		int[] ret = new int[integers.size()];
		Iterator<Integer> iterator = integers.iterator();
		for (int i = 0; i < ret.length; i++) {
			ret[i] = iterator.next().intValue();
		}
		return ret;
	}

	public static String readString(ReadableByteChannel channel)
			throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		return readString(channel, buffer);
	}

	public static String readString(ReadableByteChannel channel,
			ByteBuffer buffer) throws IOException {
		// Get the table name
		buffer.clear();
		read(channel, 4, buffer);
		int l = buffer.getInt(0);
		byte[] tmpArray = new byte[l];
		ByteBuffer tmpBuffer = ByteBuffer.wrap(tmpArray);
		int read = 0;
		while (read < l)
			read += channel.read(tmpBuffer);
		return Utils.getStringFromUtf8(tmpArray);

	}

	public static int readInt(ReadableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		return readInt(channel, buffer);
	}

	public static int readInt(ReadableByteChannel channel, ByteBuffer buffer)
			throws IOException {
		buffer.clear();
		read(channel, 4, buffer);
		return buffer.getInt(0);
	}

	public static long readLong(ReadableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		return readLong(channel, buffer);
	}

	public static long readLong(ReadableByteChannel channel, ByteBuffer buffer)
			throws IOException {
		buffer.clear();
		read(channel, 8, buffer);
		return buffer.getLong(0);

	}

	public static int read(ReadableByteChannel channel, int amount, ByteBuffer dest) throws IOException {
		int read = 0;
		int last = 0;
		if (dest.remaining() < amount) {
			throw new BufferOverflowException();
		}
		while (read < amount && last != -1) {
			last = channel.read(dest);
			if (last != -1)
				read += last;
		}
		return (read == 0 && last == -1) ? -1 : read;
	}

	public static int ensureForRead(ReadableByteChannel channel, ByteBuffer dst, int length) throws IOException {
		int res = 0;
		if (dst.remaining() < length) {
			int unread = dst.remaining();
			dst.compact();
			int fetched = 0;
			while (fetched != -1 && unread + res < length) {
				fetched = channel.read(dst);
				if (fetched != -1)
					res += fetched;
			}
			dst.flip();
			if (dst.remaining() < length) {
				throw new BufferUnderflowException();
			}
		}
		return res;
	}

	public static int ensureForWrite(WritableByteChannel dest, ByteBuffer src,
			int bytes) throws IOException {
		int numOfBytesWritten = 0;
		if (src.remaining() < bytes) {
			src.flip();
			numOfBytesWritten = dest.write(src);
			src.clear();
		}
		if (src.remaining() < bytes)
			throw new BufferOverflowException();
		return numOfBytesWritten;
	}

	public static void checkFailure(ReadableByteChannel channel, String errorMsg)
			throws IOException {
		int result = Utils.readInt(channel);
		if (result == ServerCommand.ERR.ordinal()) {
			throw new IOException(errorMsg);
		}
	}

	public static void copyBytes(ByteBuffer src, byte[] dst, int pos, int length)
			throws IOException {
		if (length <= dst.length - pos) {
			// We can just write the data to the end since we have already
			// checked that there is space enough
			src.get(dst, pos, length);
		} else {
			// Make a new tmpRow array and try again
			byte[] old = dst;
			dst = new byte[Math.max(2 * old.length, old.length + length)];
			System.arraycopy(old, 0, dst, 0, pos);
			copyBytes(src, dst, pos, length);
		}
	}


	private static int writeBytes(WritableByteChannel dst, byte[] bytes,
			ByteBuffer buf) throws IOException {
		int written = 0;
		if (bytes != null) {
			written = ensureForWrite(dst, buf, bytes.length);
			buf.put(bytes);
		}
		return written;
	}

	public static void writeRow(BufferedWriter writer, StringBuffer buf, Object[] values, int[] types,  String delim, String nullSubst) throws IOException {
		if (values == null || values.length <= 1)
			return ;
		buf.setLength(0);
		for (int i=0; i<values.length-1; ++i){
			buf.append(values[i]);
			if (i<values.length-1-1){
				buf.append(delim);
			}
		}
		writer.write(buf.toString());
		writer.newLine();
	}
	
	public static int writeRow(WritableByteChannel dst, Object[] values,
			int[] types, String delim, String nullSubst, ByteBuffer buf)
			throws IOException {
		int bytesOfWritten = 0;
		if (values == null || values.length <= 1)
			return 0;
		byte[] bytes;
		for (int i = 0; i < types.length-1; i++) {
			if (values[i] == null) {
				bytes = getBytesUtf8(nullSubst);
			} else {
				switch (types[i]) {
				case Types.BIGINT: // i.e. long
					bytes = Long.toString((Long) values[i]).getBytes();
					break;
				case Types.DATE:
					bytes = getBytesUtf8(((Date) values[i]).toString());
					break;
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.NUMERIC:
					bytes = Double.toString((Double) values[i]).getBytes();
					break;
				case Types.REAL:
					bytes = Float.toString((Float) values[i]).getBytes();
					break;
				case Types.INTEGER:
					bytes = Integer.toString((Integer) values[i]).getBytes();
					break;
				case Types.LONGVARCHAR:
				case Types.VARCHAR:
					bytes = getBytesUtf8((String) values[i]);
					break;
				default:
					throw new RuntimeException("Unexpected type found");
				}
			}

			bytesOfWritten += writeBytes(dst, bytes, buf); // Write field
			if (i < types.length - 1 - 1) { // Write delimiter
				bytes = Utils.getBytesUtf8(delim);
				bytesOfWritten += writeBytes(dst, bytes, buf);
			}
		}
		bytes = Utils.getBytesUtf8("\n");
		bytesOfWritten += writeBytes(dst, bytes, buf);
		return bytesOfWritten;
	}
	
	

	public static int writeStream(WritableByteChannel dst, Object[] values,
			int[] types, ByteBuffer buf) throws IOException {
		int bytesOfWritten = 0;
		if (values == null || values.length <= 1)
			return 0;

		for (int i = 0; i < types.length-1; i++) {
			bytesOfWritten += ensureForWrite(dst, buf, 1); // For null flag
			if (values[i] == null) {
				// Write the null flag and go on
				buf.put(IS_NULL);
				continue;
			} else {
				buf.put(IS_NOT_NULL);
			}
			// We find each type to write to the buffer. Before we write
			// to the buffer, we have to ensure that it has space enough
			// to hold a value of the data type (NB: Strings and thus Dates
			// require special treatment).
			switch (types[i]) {
			case Types.BIGINT: // i.e. long
				bytesOfWritten += ensureForWrite(dst, buf, 8);
				buf.putLong((Long) values[i]);
				break;
			case Types.DATE:
				// Write it as a string - without any length flag
				// The length is fixed to 10: YYYY-MM-DD
				// NB: For a normal string, we would check the length
				// But here we know that we use UTF-8 and only write numbers
				// and dashes which all take one byte in UTF-8.
				bytesOfWritten += ensureForWrite(dst, buf, 10);
				String dateString = ((Date) values[i]).toString();
				byte[] dateBytes = getBytesUtf8(dateString);
				buf.put(dateBytes, 0, 10);
				break;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
				bytesOfWritten += ensureForWrite(dst, buf, 8);
				buf.putDouble((Double) values[i]);
				break;
			case Types.REAL:
				bytesOfWritten += ensureForWrite(dst, buf, 4);
				buf.putFloat((Float) values[i]);
				break;
			case Types.INTEGER:
				bytesOfWritten += ensureForWrite(dst, buf, 4);
				buf.putInt((Integer) values[i]);
				break;
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				// Write the length of the bytes encoded as UTF-8.
				// Then write the bytes
				byte[] ascii = getBytesUtf8((String) values[i]);
				bytesOfWritten += ensureForWrite(dst, buf, 4 + ascii.length); // Length
																				// +
																				// the
																				// bytes
				buf.putInt(ascii.length).put(ascii, 0, ascii.length);
				break;
			default:
				throw new RuntimeException("Unexpected type found");
			}
		}
		return bytesOfWritten;
	}

	
public static Object[] readRow(ReadableByteChannel channel, int[] types, ByteBuffer dst) {
try {
	Object[] retVals = new Object[types.length];
	for (int i = 0; i < types.length; ++i) {
		Utils.ensureForRead(channel, dst, 1);
	     if  (dst.get() == IS_NULL){
	    	 retVals[i] = null;
	    	 continue;
	     }
	     
		switch (types[i]) {
		case Types.BIGINT: // i.e. long
			Utils.ensureForRead(channel, dst, 8);
			retVals[i] = dst.getLong();
			break;
		case Types.DATE:
			// Read it as a string - without any length flag
			// The length is fixed to 10: YYYY-MM-DD
			Utils.ensureForRead(channel, dst, 10);
			byte[] ascii = new byte[10];
			dst.get(ascii, 0, 10);
			retVals[i] = Date.valueOf(new String(ascii, 0, 10,	"UTF-8"));
			break;
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
			Utils.ensureForRead(channel, dst, 8);
			retVals[i] = dst.getDouble();
			break;
		case Types.REAL:
			Utils.ensureForRead(channel, dst, 4);
			retVals[i] = dst.getFloat();
			break;
		case Types.INTEGER:
			Utils.ensureForRead(channel, dst, 4);
			retVals[i] = dst.getInt();
			break;
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
			Utils.ensureForRead(channel, dst, 4);
			int l = dst.getInt();
			Utils.ensureForRead(channel, dst, l);
			ascii = new byte[l]; // Make a new temporary then
			dst.get(ascii, 0, l);
			retVals[i] = new String(ascii, 0, l, "UTF-8");
			break;
		default:
			throw new RiteException("Unexpected type found");
		}
	}
	return retVals;
  } catch (Exception e) {
	e.printStackTrace();
	return null;
  }
}

	
	public static byte[] getBytesFromFile(File file) throws IOException {
		InputStream is = new FileInputStream(file);

		// Get the size of the file
		long length = file.length();

		if (length > Integer.MAX_VALUE) {
			// File is too large
		}

		// Create the byte array to hold the data
		byte[] bytes = new byte[(int) length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}

		// Ensure all the bytes have been read in
		if (offset < bytes.length) {
			throw new IOException("Could not completely read file "
					+ file.getName());
		}

		// Close the input stream and return bytes
		is.close();
		return bytes;
	}

	public static long parseLong(char[] chrs) {
		long result = 0;
		int i = 0, max = chrs.length;
		int digit;

		if (max > 0) {
			if (i < max) {
				digit = Character.digit(chrs[i++], 2);
				result = -digit;
			}
			while (i < max) {
				digit = Character.digit(chrs[i++], 2);
				result *= 2;
				result -= digit;
			}
		}
		return -result;
	}

	public static final byte[] intToByteArray(int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16),
				(byte) (value >>> 8), (byte) value };
	}
	
	public final byte[] longToBytes(long v) {
	    byte[] writeBuffer = new byte[ 8 ];

	    writeBuffer[0] = (byte)(v >>> 56);
	    writeBuffer[1] = (byte)(v >>> 48);
	    writeBuffer[2] = (byte)(v >>> 40);
	    writeBuffer[3] = (byte)(v >>> 32);
	    writeBuffer[4] = (byte)(v >>> 24);
	    writeBuffer[5] = (byte)(v >>> 16);
	    writeBuffer[6] = (byte)(v >>>  8);
	    writeBuffer[7] = (byte)(v >>>  0);

	    return writeBuffer;
	}
	 /**
     * Convert the byte array to an int starting from the given offset.
     *
     * @param b The byte array
     * @param offset The array offset
     * @return The integer
     */
	public static int byteArrayToInt(byte[] b, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i + offset] & 0x000000FF) << shift;
        }
        return value;
    }
	
	public static char[] parseLongToAcsii(long i) {
		char[] buf = new char[64];
		int charPos = 64;
		int radix = 1 << 1;
		long mask = radix - 1;
		do {
			buf[--charPos] = digits[(int) (i & mask)];
			i >>>= 1;
		} while (i != 0);
		int l = 64 - charPos;
		char[] dst = new char[l];
		System.arraycopy(buf, charPos, dst, 0, l);
		return dst;
	}

	public static byte[] getBytesUtf8(String string)
			throws UnsupportedEncodingException {
		if (string == null) {
			return null;
		}
		return string.getBytes("UTF-8");
	}

	public static String getStringFromUtf8(byte[] utf8Bytes) {
		return new String(utf8Bytes, Charset.forName("UTF-8"));
	}

	public static String getStringFromUtf8(byte[] utf8Bytes, int offset,
			int length) {
		return new String(utf8Bytes, offset, length, Charset.forName("UTF-8"));
	}

	public static void main(String[] args) {
		long l = Utils.parseLong(new char[] { '2', '3', '5' });
		System.out.println(l);
		char[] chs = Utils.parseLongToAcsii(l);
		for (char ch : chs) {
			System.out.println(ch);
		}
	}

	public static void resp(SocketChannel channel, ByteBuffer buffer)
			throws IOException {
		buffer.clear();
		buffer.put((byte) 1);
		buffer.flip();
		channel.write(buffer);
	}

	public static void resp(SocketChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(1);
		resp(channel, buffer);
	}

	public static boolean recv(SocketChannel channel, ByteBuffer buffer)
			throws IOException {
		buffer.clear();
		channel.read(buffer);
		return buffer.get(0) == (byte) 1;
	}

	public static boolean recv(SocketChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(1);
		return recv(channel, buffer);
	}

	public static void bye(SocketChannel channel, ByteBuffer buf) {
		try {
			buf.clear();
			buf.putInt(ServerCommand.BYE.ordinal()).flip();
			channel.write(buf);
			channel.close();
		} catch (Exception e) {
		}
	}

	public static void send(ByteChannel channel, ByteBuffer buf,
			ServerCommand command) throws IOException {
		buf.clear();
		buf.putInt(command.ordinal());
		buf.flip();
		channel.write(buf);
	}

	public static void closeQuietly(ByteChannel channel) {
		try {
			channel.close();
		} catch (Exception e) {
		}
	}

	public static void closeQuietly(Connection conn) {
		try {
			conn.close();
		} catch (Exception e) {
		}
	}

	public static void execShellCmd(List<String> commands, String workingDir, boolean wait) throws IOException,
			InterruptedException {
		try {
			// Run macro on target
			ProcessBuilder pb = new ProcessBuilder(commands);
			pb.directory(new File(workingDir));
			pb.redirectErrorStream(true);
			Process process = pb.start();

			// Read output
			StringBuilder out = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					process.getInputStream()));
			String line = null, previous = null;
			while ((line = br.readLine()) != null)
				if (!line.equals(previous)) {
					previous = line;
					out.append(line).append('\n');
					System.out.println(line);
				}

			// Check result
			if (wait && process.waitFor() == 0)
				System.out.println("Success!");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(commands);
		}
	}

	public static byte findType(Object o) {
		if (o == null) {
			return NULL;
		}
		if (o instanceof String) {
			return CHARARRAY;
		} else if (o instanceof Integer) {
			return INTEGER;
		} else if (o instanceof Long) {
			return LONG;
		} else if (o instanceof Float) {
			return FLOAT;
		} else if (o instanceof Double) {
			return DOUBLE;
		} else if (o instanceof Boolean) {
			return BOOLEAN;
		} else if (o instanceof Byte) {
			return BYTE;
		} else {
			return ERROR;
		}
	}

	public static int compare(Object o1, Object o2) {
		byte dt1 = findType(o1);
		byte dt2 = findType(o2);
		return compare(o1, o2, dt1, dt2);
	}

	@SuppressWarnings("unchecked")
	public static int compare(Object o1, Object o2, byte dt1, byte dt2) {
		if (dt1 == dt2) {
			switch (dt1) {
			case NULL:
				return 0;
			case BOOLEAN:
				return ((Boolean) o1).compareTo((Boolean) o2);
			case BYTE:
				return ((Byte) o1).compareTo((Byte) o2);
			case INTEGER:
				return ((Integer) o1).compareTo((Integer) o2);
			case LONG:
				return ((Long) o1).compareTo((Long) o2);
			case FLOAT:
				return ((Float) o1).compareTo((Float) o2);
			case DOUBLE:
				return ((Double) o1).compareTo((Double) o2);
			case CHARARRAY:
				return ((String) o1).compareTo((String) o2);
			default:
				throw new RuntimeException("Unkown type " + dt1 + " in compare");
			}
		} else if (dt1 < dt2) {
			return -1;
		} else {
			return 1;
		}
	}


	public static void mkdir(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                String message =
                    "File "
                        + directory
                        + " exists and is "
                        + "not a directory. Unable to create directory.";
                throw new IOException(message);
            }
        } else {
            if (!directory.mkdirs()) {
                // Double-check that some other thread or process hasn't made
                // the directory in the background
                if (!directory.isDirectory())
                {
                    String message =
                        "Unable to create directory " + directory;
                    throw new IOException(message);
                }
            }
        }
    }
	
	public static void deleteDirectory(File directory) throws IOException {
		if (!directory.exists()) {
			return;
		}
		cleanDirectory(directory);
		if (!directory.delete()) {
			String message = "Unable to delete directory " + directory + ".";
			throw new IOException(message);
		}
	}

	public static void cleanDirectory(File directory) throws IOException {
		if (!directory.exists()) {
			String message = directory + " does not exist";
			throw new IllegalArgumentException(message);
		}

		if (!directory.isDirectory()) {
			String message = directory + " is not a directory";
			throw new IllegalArgumentException(message);
		}

		File[] files = directory.listFiles();
		if (files == null) { // null if security restricted
			throw new IOException("Failed to list contents of " + directory);
		}

		IOException exception = null;
		for (File file : files) {
			try {
				forceDelete(file);
			} catch (IOException ioe) {
				exception = ioe;
			}
		}

		if (null != exception) {
			throw exception;
		}
	}
	
	public static void forceDelete(File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            boolean filePresent = file.exists();
            if (!file.delete()) {
                if (!filePresent){
                    throw new FileNotFoundException("File does not exist: " + file);
                }
                String message =
                    "Unable to delete file: " + file;
                throw new IOException(message);
            }
        }
    }
	

}
