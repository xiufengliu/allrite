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

package dk.aau.cs.rite.sqlparser;



public interface ISqlParserConstants {

  /** End of File. */
  int EOF = 0;
  /** RegularExpression Id. */
  int K_AS = 5;
  /** RegularExpression Id. */
  int K_BY = 6;
  /** RegularExpression Id. */
  int K_DO = 7;
  /** RegularExpression Id. */
  int K_IS = 8;
  /** RegularExpression Id. */
  int K_IN = 9;
  /** RegularExpression Id. */
  int K_OR = 10;
  /** RegularExpression Id. */
  int K_ON = 11;
  /** RegularExpression Id. */
  int K_ALL = 12;
  /** RegularExpression Id. */
  int K_AND = 13;
  /** RegularExpression Id. */
  int K_ANY = 14;
  /** RegularExpression Id. */
  int K_KEY = 15;
  /** RegularExpression Id. */
  int K_NOT = 16;
  /** RegularExpression Id. */
  int K_SET = 17;
  /** RegularExpression Id. */
  int K_ASC = 18;
  /** RegularExpression Id. */
  int K_TOP = 19;
  /** RegularExpression Id. */
  int K_END = 20;
  /** RegularExpression Id. */
  int K_DESC = 21;
  /** RegularExpression Id. */
  int K_INTO = 22;
  /** RegularExpression Id. */
  int K_NULL = 23;
  /** RegularExpression Id. */
  int K_LIKE = 24;
  /** RegularExpression Id. */
  int K_DROP = 25;
  /** RegularExpression Id. */
  int K_JOIN = 26;
  /** RegularExpression Id. */
  int K_LEFT = 27;
  /** RegularExpression Id. */
  int K_FROM = 28;
  /** RegularExpression Id. */
  int K_OPEN = 29;
  /** RegularExpression Id. */
  int K_CASE = 30;
  /** RegularExpression Id. */
  int K_WHEN = 31;
  /** RegularExpression Id. */
  int K_THEN = 32;
  /** RegularExpression Id. */
  int K_ELSE = 33;
  /** RegularExpression Id. */
  int K_SOME = 34;
  /** RegularExpression Id. */
  int K_FULL = 35;
  /** RegularExpression Id. */
  int K_WITH = 36;
  /** RegularExpression Id. */
  int K_TABLE = 37;
  /** RegularExpression Id. */
  int K_WHERE = 38;
  /** RegularExpression Id. */
  int K_USING = 39;
  /** RegularExpression Id. */
  int K_UNION = 40;
  /** RegularExpression Id. */
  int K_GROUP = 41;
  /** RegularExpression Id. */
  int K_BEGIN = 42;
  /** RegularExpression Id. */
  int K_INDEX = 43;
  /** RegularExpression Id. */
  int K_INNER = 44;
  /** RegularExpression Id. */
  int K_LIMIT = 45;
  /** RegularExpression Id. */
  int K_OUTER = 46;
  /** RegularExpression Id. */
  int K_ORDER = 47;
  /** RegularExpression Id. */
  int K_RIGHT = 48;
  /** RegularExpression Id. */
  int K_DELETE = 49;
  /** RegularExpression Id. */
  int K_CREATE = 50;
  /** RegularExpression Id. */
  int K_SELECT = 51;
  /** RegularExpression Id. */
  int K_OFFSET = 52;
  /** RegularExpression Id. */
  int K_EXISTS = 53;
  /** RegularExpression Id. */
  int K_HAVING = 54;
  /** RegularExpression Id. */
  int K_INSERT = 55;
  /** RegularExpression Id. */
  int K_UPDATE = 56;
  /** RegularExpression Id. */
  int K_VALUES = 57;
  /** RegularExpression Id. */
  int K_ESCAPE = 58;
  /** RegularExpression Id. */
  int K_PRIMARY = 59;
  /** RegularExpression Id. */
  int K_NATURAL = 60;
  /** RegularExpression Id. */
  int K_REPLACE = 61;
  /** RegularExpression Id. */
  int K_BETWEEN = 62;
  /** RegularExpression Id. */
  int K_TRUNCATE = 63;
  /** RegularExpression Id. */
  int K_DISTINCT = 64;
  /** RegularExpression Id. */
  int K_INTERSECT = 65;
  /** RegularExpression Id. */
  int S_DOUBLE = 66;
  /** RegularExpression Id. */
  int S_INTEGER = 67;
  /** RegularExpression Id. */
  int DIGIT = 68;
  /** RegularExpression Id. */
  int LINE_COMMENT = 69;
  /** RegularExpression Id. */
  int MULTI_LINE_COMMENT = 70;
  /** RegularExpression Id. */
  int S_IDENTIFIER = 71;
  /** RegularExpression Id. */
  int LETTER = 72;
  /** RegularExpression Id. */
  int SPECIAL_CHARS = 73;
  /** RegularExpression Id. */
  int S_CHAR_LITERAL = 74;
  /** RegularExpression Id. */
  int S_QUOTED_IDENTIFIER = 75;

  /** Lexical state. */
  int DEFAULT = 0;

  /** Literal token values. */
  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\r\"",
    "\"\\n\"",
    "\"AS\"",
    "\"BY\"",
    "\"DO\"",
    "\"IS\"",
    "\"IN\"",
    "\"OR\"",
    "\"ON\"",
    "\"ALL\"",
    "\"AND\"",
    "\"ANY\"",
    "\"KEY\"",
    "\"NOT\"",
    "\"SET\"",
    "\"ASC\"",
    "\"TOP\"",
    "\"END\"",
    "\"DESC\"",
    "\"INTO\"",
    "\"NULL\"",
    "\"LIKE\"",
    "\"DROP\"",
    "\"JOIN\"",
    "\"LEFT\"",
    "\"FROM\"",
    "\"OPEN\"",
    "\"CASE\"",
    "\"WHEN\"",
    "\"THEN\"",
    "\"ELSE\"",
    "\"SOME\"",
    "\"FULL\"",
    "\"WITH\"",
    "\"TABLE\"",
    "\"WHERE\"",
    "\"USING\"",
    "\"UNION\"",
    "\"GROUP\"",
    "\"BEGIN\"",
    "\"INDEX\"",
    "\"INNER\"",
    "\"LIMIT\"",
    "\"OUTER\"",
    "\"ORDER\"",
    "\"RIGHT\"",
    "\"DELETE\"",
    "\"CREATE\"",
    "\"SELECT\"",
    "\"OFFSET\"",
    "\"EXISTS\"",
    "\"HAVING\"",
    "\"INSERT\"",
    "\"UPDATE\"",
    "\"VALUES\"",
    "\"ESCAPE\"",
    "\"PRIMARY\"",
    "\"NATURAL\"",
    "\"REPLACE\"",
    "\"BETWEEN\"",
    "\"TRUNCATE\"",
    "\"DISTINCT\"",
    "\"INTERSECT\"",
    "<S_DOUBLE>",
    "<S_INTEGER>",
    "<DIGIT>",
    "<LINE_COMMENT>",
    "<MULTI_LINE_COMMENT>",
    "<S_IDENTIFIER>",
    "<LETTER>",
    "<SPECIAL_CHARS>",
    "<S_CHAR_LITERAL>",
    "<S_QUOTED_IDENTIFIER>",
    "\";\"",
    "\"=\"",
    "\",\"",
    "\"(\"",
    "\")\"",
    "\".\"",
    "\"*\"",
    "\"?\"",
    "\">\"",
    "\"<\"",
    "\">=\"",
    "\"<=\"",
    "\"<>\"",
    "\"!=\"",
    "\"@@\"",
    "\"||\"",
    "\"|\"",
    "\"&\"",
    "\"+\"",
    "\"-\"",
    "\"/\"",
    "\"^\"",
    "\"{d\"",
    "\"}\"",
    "\"{t\"",
    "\"{ts\"",
    "\"{fn\"",
  };

}
