package qp.parser;
import java_cup.runtime.Symbol;  // definition of scanner/parser interface
import java.util.*;


public class Scaner implements java_cup.runtime.Scanner {
	private final int YY_BUFFER_SIZE = 512;
	private final int YY_F = -1;
	private final int YY_NO_STATE = -1;
	private final int YY_NOT_ACCEPT = 0;
	private final int YY_START = 1;
	private final int YY_END = 2;
	private final int YY_NO_ANCHOR = 4;
	private final int YY_BOL = 128;
	private final int YY_EOF = 129;
	private java.io.BufferedReader yy_reader;
	private int yy_buffer_index;
	private int yy_buffer_read;
	private int yy_buffer_start;
	private int yy_buffer_end;
	private char yy_buffer[];
	private int yychar;
	private int yyline;
	private boolean yy_at_bol;
	private int yy_lexical_state;

	public Scaner (java.io.Reader reader) {
		this ();
		if (null == reader) {
			throw (new Error("Error: Bad input stream initializer."));
		}
		yy_reader = new java.io.BufferedReader(reader);
	}

	public Scaner (java.io.InputStream instream) {
		this ();
		if (null == instream) {
			throw (new Error("Error: Bad input stream initializer."));
		}
		yy_reader = new java.io.BufferedReader(new java.io.InputStreamReader(instream));
	}

	private Scaner () {
		yy_buffer = new char[YY_BUFFER_SIZE];
		yy_buffer_read = 0;
		yy_buffer_index = 0;
		yy_buffer_start = 0;
		yy_buffer_end = 0;
		yychar = 0;
		yyline = 0;
		yy_at_bol = true;
		yy_lexical_state = YYINITIAL;
	}

	private boolean yy_eof_done = false;
	private final int YYINITIAL = 0;
	private final int NEGATE = 1;
	private final int yy_state_dtrans[] = {
		0,
		0
	};
	private void yybegin (int state) {
		yy_lexical_state = state;
	}
	private int yy_advance ()
		throws java.io.IOException {
		int next_read;
		int i;
		int j;

		if (yy_buffer_index < yy_buffer_read) {
			return yy_buffer[yy_buffer_index++];
		}

		if (0 != yy_buffer_start) {
			i = yy_buffer_start;
			j = 0;
			while (i < yy_buffer_read) {
				yy_buffer[j] = yy_buffer[i];
				++i;
				++j;
			}
			yy_buffer_end = yy_buffer_end - yy_buffer_start;
			yy_buffer_start = 0;
			yy_buffer_read = j;
			yy_buffer_index = j;
			next_read = yy_reader.read(yy_buffer,
					yy_buffer_read,
					yy_buffer.length - yy_buffer_read);
			if (-1 == next_read) {
				return YY_EOF;
			}
			yy_buffer_read = yy_buffer_read + next_read;
		}

		while (yy_buffer_index >= yy_buffer_read) {
			if (yy_buffer_index >= yy_buffer.length) {
				yy_buffer = yy_double(yy_buffer);
			}
			next_read = yy_reader.read(yy_buffer,
					yy_buffer_read,
					yy_buffer.length - yy_buffer_read);
			if (-1 == next_read) {
				return YY_EOF;
			}
			yy_buffer_read = yy_buffer_read + next_read;
		}
		return yy_buffer[yy_buffer_index++];
	}
	private void yy_move_end () {
		if (yy_buffer_end > yy_buffer_start &&
		    '\n' == yy_buffer[yy_buffer_end-1])
			yy_buffer_end--;
		if (yy_buffer_end > yy_buffer_start &&
		    '\r' == yy_buffer[yy_buffer_end-1])
			yy_buffer_end--;
	}
	private boolean yy_last_was_cr=false;
	private void yy_mark_start () {
		int i;
		for (i = yy_buffer_start; i < yy_buffer_index; ++i) {
			if ('\n' == yy_buffer[i] && !yy_last_was_cr) {
				++yyline;
			}
			if ('\r' == yy_buffer[i]) {
				++yyline;
				yy_last_was_cr=true;
			} else yy_last_was_cr=false;
		}
		yychar = yychar
			+ yy_buffer_index - yy_buffer_start;
		yy_buffer_start = yy_buffer_index;
	}
	private void yy_mark_end () {
		yy_buffer_end = yy_buffer_index;
	}
	private void yy_to_mark () {
		yy_buffer_index = yy_buffer_end;
		yy_at_bol = (yy_buffer_end > yy_buffer_start) &&
		            ('\r' == yy_buffer[yy_buffer_end-1] ||
		             '\n' == yy_buffer[yy_buffer_end-1] ||
		             2028/*LS*/ == yy_buffer[yy_buffer_end-1] ||
		             2029/*PS*/ == yy_buffer[yy_buffer_end-1]);
	}
	private java.lang.String yytext () {
		return (new java.lang.String(yy_buffer,
			yy_buffer_start,
			yy_buffer_end - yy_buffer_start));
	}
	private int yylength () {
		return yy_buffer_end - yy_buffer_start;
	}
	private char[] yy_double (char buf[]) {
		int i;
		char newbuf[];
		newbuf = new char[2*buf.length];
		for (i = 0; i < buf.length; ++i) {
			newbuf[i] = buf[i];
		}
		return newbuf;
	}
	private final int YY_E_INTERNAL = 0;
	private final int YY_E_MATCH = 1;
	private java.lang.String yy_error_string[] = {
		"Error: Internal error.\n",
		"Error: Unmatched input.\n"
	};
	private void yy_error (int code,boolean fatal) {
		java.lang.System.out.print(yy_error_string[code]);
		java.lang.System.out.flush();
		if (fatal) {
			throw new Error("Fatal Error.\n");
		}
	}
	private int[][] unpackFromString(int size1, int size2, String st) {
		int colonIndex = -1;
		String lengthString;
		int sequenceLength = 0;
		int sequenceInteger = 0;

		int commaIndex;
		String workString;

		int res[][] = new int[size1][size2];
		for (int i= 0; i < size1; i++) {
			for (int j= 0; j < size2; j++) {
				if (sequenceLength != 0) {
					res[i][j] = sequenceInteger;
					sequenceLength--;
					continue;
				}
				commaIndex = st.indexOf(',');
				workString = (commaIndex==-1) ? st :
					st.substring(0, commaIndex);
				st = st.substring(commaIndex+1);
				colonIndex = workString.indexOf(':');
				if (colonIndex == -1) {
					res[i][j]=Integer.parseInt(workString);
					continue;
				}
				lengthString =
					workString.substring(colonIndex+1);
				sequenceLength=Integer.parseInt(lengthString);
				workString=workString.substring(0,colonIndex);
				sequenceInteger=Integer.parseInt(workString);
				res[i][j] = sequenceInteger;
				sequenceLength--;
			}
		}
		return res;
	}
	private int yy_acpt[] = {
		/* 0 */ YY_NOT_ACCEPT,
		/* 1 */ YY_NO_ANCHOR,
		/* 2 */ YY_NO_ANCHOR,
		/* 3 */ YY_NO_ANCHOR,
		/* 4 */ YY_NO_ANCHOR,
		/* 5 */ YY_NO_ANCHOR,
		/* 6 */ YY_NO_ANCHOR,
		/* 7 */ YY_NO_ANCHOR,
		/* 8 */ YY_NO_ANCHOR,
		/* 9 */ YY_NO_ANCHOR,
		/* 10 */ YY_NO_ANCHOR,
		/* 11 */ YY_NO_ANCHOR,
		/* 12 */ YY_NO_ANCHOR,
		/* 13 */ YY_NO_ANCHOR,
		/* 14 */ YY_NO_ANCHOR,
		/* 15 */ YY_NO_ANCHOR,
		/* 16 */ YY_NO_ANCHOR,
		/* 17 */ YY_NO_ANCHOR,
		/* 18 */ YY_NO_ANCHOR,
		/* 19 */ YY_NO_ANCHOR,
		/* 20 */ YY_NO_ANCHOR,
		/* 21 */ YY_NO_ANCHOR,
		/* 22 */ YY_NOT_ACCEPT,
		/* 23 */ YY_NO_ANCHOR,
		/* 24 */ YY_NOT_ACCEPT,
		/* 25 */ YY_NO_ANCHOR,
		/* 26 */ YY_NOT_ACCEPT,
		/* 27 */ YY_NO_ANCHOR,
		/* 28 */ YY_NOT_ACCEPT,
		/* 29 */ YY_NO_ANCHOR,
		/* 30 */ YY_NOT_ACCEPT,
		/* 31 */ YY_NO_ANCHOR,
		/* 32 */ YY_NO_ANCHOR,
		/* 33 */ YY_NO_ANCHOR,
		/* 34 */ YY_NO_ANCHOR,
		/* 35 */ YY_NO_ANCHOR,
		/* 36 */ YY_NO_ANCHOR,
		/* 37 */ YY_NO_ANCHOR,
		/* 38 */ YY_NO_ANCHOR,
		/* 39 */ YY_NO_ANCHOR,
		/* 40 */ YY_NO_ANCHOR,
		/* 41 */ YY_NO_ANCHOR,
		/* 42 */ YY_NO_ANCHOR,
		/* 43 */ YY_NO_ANCHOR,
		/* 44 */ YY_NO_ANCHOR,
		/* 45 */ YY_NO_ANCHOR,
		/* 46 */ YY_NO_ANCHOR,
		/* 47 */ YY_NO_ANCHOR,
		/* 48 */ YY_NO_ANCHOR,
		/* 49 */ YY_NO_ANCHOR,
		/* 50 */ YY_NO_ANCHOR,
		/* 51 */ YY_NO_ANCHOR,
		/* 52 */ YY_NO_ANCHOR
	};
	private int yy_cmap[] = unpackFromString(1,130,
"0:9,18:2,0,18:2,0:18,22,23,21,24:3,33,26,24:2,17,24,28,24,35,24,20:10,24,30" +
",31,29,32,24:2,19,13,4,12,2,6,19,11,15,19:2,3,9,16,8,19:2,7,1,5,19:2,10,19," +
"14,19,24,25,24:2,19,24,19:13,27,19:5,27,19:6,24,34,24:2,0,36:2")[0];

	private int yy_rmap[] = unpackFromString(1,53,
"0,1,2,3,2:3,4,5,2:8,6:5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24," +
"25,26,27,28,6,29,30,31,32,33,6,34,35")[0];

	private int yy_nxt[][] = unpackFromString(36,37,
"-1,1,50:4,37,50,51,50,42,50,52,50:4,2,3,50,-1,22,3,24,-1:3,50,4,5,6,7,8,26," +
"28,9,10,-1,50,43,50:14,-1:2,50,44,-1:6,50,-1:64,3,-1:3,3,-1:43,13,-1:36,14," +
"-1:8,50:16,-1:2,50,44,-1:6,50,-1:10,22:17,-1,22:2,11,22:3,30,-1,22:9,-1:2,5" +
"0:8,17,50:7,-1:2,50,44,-1:6,50,-1:38,12,-1:8,50,18,50:14,-1:2,50,44,-1:6,50" +
",-1:42,15,-1:4,50:4,19,50:11,-1:2,50,44,-1:6,50,-1:43,16,-1:3,50:13,20,50:2" +
",-1:2,50,44,-1:6,50,-1:30,22,-1:3,22:3,-1:10,50:4,21,50:11,-1:2,50,44,-1:6," +
"50,-1:10,50:7,23,50:8,-1:2,50,44,-1:6,50,-1:10,50:6,25,50:9,-1:2,50,44,-1:6" +
",50,-1:10,50:3,27,50:12,-1:2,50,44,-1:6,50,-1:10,50:12,29,50:3,-1:2,50,44,-" +
"1:6,50,-1:10,50:3,31,50:12,-1:2,50,44,-1:6,50,-1:10,50:6,32,50:9,-1:2,50,44" +
",-1:6,50,-1:10,50,33,50:14,-1:2,50,44,-1:6,50,-1:10,50,34,50:14,-1:2,50,44," +
"-1:6,50,-1:10,50:6,35,50:9,-1:2,50,44,-1:6,50,-1:10,50:15,36,-1:2,50,44,-1:" +
"6,50,-1:10,50:10,38,50:5,-1:2,50,44,-1:6,50,-1:10,50:2,39,50:13,-1:2,50,44," +
"-1:6,50,-1:10,50:11,47,50:4,-1:2,50,44,-1:6,50,-1:10,48,50:15,-1:2,50,44,-1" +
":6,50,-1:10,50,40,50:14,-1:2,50,44,-1:6,50,-1:10,50:4,49,50:11,-1:2,50,44,-" +
"1:6,50,-1:10,50:14,41,50,-1:2,50,44,-1:6,50,-1:10,50:6,45,50:9,-1:2,50,44,-" +
"1:6,50,-1:10,50:14,46,50,-1:2,50,44,-1:6,50,-1:9");

	public java_cup.runtime.Symbol next_token ()
		throws java.io.IOException {
		int yy_lookahead;
		int yy_anchor = YY_NO_ANCHOR;
		int yy_state = yy_state_dtrans[yy_lexical_state];
		int yy_next_state = YY_NO_STATE;
		int yy_last_accept_state = YY_NO_STATE;
		boolean yy_initial = true;
		int yy_this_accept;

		yy_mark_start();
		yy_this_accept = yy_acpt[yy_state];
		if (YY_NOT_ACCEPT != yy_this_accept) {
			yy_last_accept_state = yy_state;
			yy_mark_end();
		}
		while (true) {
			if (yy_initial && yy_at_bol) yy_lookahead = YY_BOL;
			else yy_lookahead = yy_advance();
			yy_next_state = YY_F;
			yy_next_state = yy_nxt[yy_rmap[yy_state]][yy_cmap[yy_lookahead]];
			if (YY_EOF == yy_lookahead && true == yy_initial) {

  return new Symbol(sym.EOF, new TokenValue("<EOF>"));
			}
			if (YY_F != yy_next_state) {
				yy_state = yy_next_state;
				yy_initial = false;
				yy_this_accept = yy_acpt[yy_state];
				if (YY_NOT_ACCEPT != yy_this_accept) {
					yy_last_accept_state = yy_state;
					yy_mark_end();
				}
			}
			else {
				if (YY_NO_STATE == yy_last_accept_state) {
					throw (new Error("Lexical Error: Unmatched Input."));
				}
				else {
					yy_anchor = yy_acpt[yy_last_accept_state];
					if (0 != (YY_END & yy_anchor)) {
						yy_move_end();
					}
					yy_to_mark();
					switch (yy_last_accept_state) {
					case 1:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -2:
						break;
					case 2:
						{
    yybegin(YYINITIAL);
    return new Symbol(sym.STAR,yyline,yychar,new TokenValue(yytext()));
}
					case -3:
						break;
					case 3:
						{ 
}
					case -4:
						break;
					case 4:
						{
  yybegin(NEGATE); 
  return new Symbol(sym.COMMA, yyline,yychar,new TokenValue(yytext())); 
}
					case -5:
						break;
					case 5:
						{
  yybegin(NEGATE); 
  return new Symbol(sym.EQUAL, yyline,yychar,new TokenValue(yytext()));
}
					case -6:
						break;
					case 6:
						{
  yybegin(YYINITIAL); 
  return new Symbol(sym.SEMI, yyline,yychar,new TokenValue(yytext()));
}
					case -7:
						break;
					case 7:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.LESSTHAN,yyline,yychar,new TokenValue(yytext()));
}
					case -8:
						break;
					case 8:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.GREATERTHAN,yyline,yychar,new TokenValue(yytext()));
}
					case -9:
						break;
					case 9:
						{
  yybegin(YYINITIAL);
   return new Symbol(sym.DOT,yyline,yychar,new TokenValue(yytext()));
}
					case -10:
						break;
					case 10:
						
					case -11:
						break;
					case 11:
						{ 
  yybegin(YYINITIAL); 
    return new Symbol(sym.STRINGLIT,yyline,yychar, new TokenValue(yytext().substring(1,yytext().length()-1))); 
  }
					case -12:
						break;
					case 12:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.NOTEQUAL, yyline,yychar,new TokenValue(yytext()));
}
					case -13:
						break;
					case 13:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.LTOE,yyline,yychar,new TokenValue(yytext()));
}
					case -14:
						break;
					case 14:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.GTOE, yyline,yychar,new TokenValue(yytext()));
}
					case -15:
						break;
					case 15:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.AND, yyline,yychar,new TokenValue(yytext()));
}
					case -16:
						break;
					case 16:
						{ 
  yybegin(NEGATE);
  return new Symbol(sym.OR,yyline,yychar,new TokenValue(yytext()));
}
					case -17:
						break;
					case 17:
						{
  yybegin(YYINITIAL);
  return new Symbol(sym.FROM,yyline,yychar,new TokenValue(yytext()));
}
					case -18:
						break;
					case 18:
						{
  yybegin(YYINITIAL);
  return new Symbol(sym.WHERE,yyline,yychar,new TokenValue(yytext()));
}
					case -19:
						break;
					case 19:
						{
  yybegin(YYINITIAL);
  return new Symbol(sym.SELECT,yyline,yychar,new TokenValue(yytext()));
}
					case -20:
						break;
					case 20:
						{
  yybegin(YYINITIAL);
  return new Symbol(sym.ORDERBY,yyline,yychar,new TokenValue(yytext()));
}
					case -21:
						break;
					case 21:
						{
  yybegin(YYINITIAL);
  return new Symbol(sym.DISTINCT,yyline,yychar,new TokenValue(yytext()));
}
					case -22:
						break;
					case 23:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -23:
						break;
					case 25:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -24:
						break;
					case 27:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -25:
						break;
					case 29:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -26:
						break;
					case 31:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -27:
						break;
					case 32:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -28:
						break;
					case 33:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -29:
						break;
					case 34:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -30:
						break;
					case 35:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -31:
						break;
					case 36:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -32:
						break;
					case 37:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -33:
						break;
					case 38:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -34:
						break;
					case 39:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -35:
						break;
					case 40:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -36:
						break;
					case 41:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -37:
						break;
					case 42:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -38:
						break;
					case 43:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -39:
						break;
					case 44:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -40:
						break;
					case 45:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -41:
						break;
					case 46:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -42:
						break;
					case 47:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -43:
						break;
					case 48:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -44:
						break;
					case 49:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -45:
						break;
					case 50:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -46:
						break;
					case 51:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -47:
						break;
					case 52:
						{ 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}
					case -48:
						break;
					default:
						yy_error(YY_E_INTERNAL,false);
					case -1:
					}
					yy_initial = true;
					yy_state = yy_state_dtrans[yy_lexical_state];
					yy_next_state = YY_NO_STATE;
					yy_last_accept_state = YY_NO_STATE;
					yy_mark_start();
					yy_this_accept = yy_acpt[yy_state];
					if (YY_NOT_ACCEPT != yy_this_accept) {
						yy_last_accept_state = yy_state;
						yy_mark_end();
					}
				}
			}
		}
	}
}
