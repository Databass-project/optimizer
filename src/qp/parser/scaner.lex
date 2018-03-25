
import java_cup.runtime.Symbol;  // definition of scanner/parser interface
import java.util.*;
 
%%

%eofval{
  return new Symbol(sym.EOF, new TokenValue("<EOF>"));
%eofval}
%public
%line
%char
%implements java_cup.runtime.Scanner
%function next_token
%type java_cup.runtime.Symbol
%state NEGATE



ALPHA=[A-Za-z_]
DIGIT=[0-9]
WHITE_SPACE=([\ \r\t\f\n])+
DECIMAL_LITERAL=({DIGIT})+
INT_LITERAL={DECIMAL_LITERAL}
ALPHA_NUMERIC={ALPHA}|{DIGIT}
ID={ALPHA}({ALPHA_NUMERIC})*
CHAR=([\040-\041]|[\043-\046]|[\050-\133]|[\135-\176]|\\\\|\\'|\\\"|"\t"|"\n")
CHAR_LITERAL='{CHAR}'
STRING_LITERAL=\"{CHAR}*\"


 
%%



<YYINITIAL,NEGATE> SELECT {
  yybegin(YYINITIAL);
  return new Symbol(sym.SELECT,yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> FROM {
  yybegin(YYINITIAL);
  return new Symbol(sym.FROM,yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> WHERE {
  yybegin(YYINITIAL);
  return new Symbol(sym.WHERE,yyline,yychar,new TokenValue(yytext()));
}
   

<YYINITIAL,NEGATE> GROUPBY {
  yybegin(YYINITIAL);
  return new Symbol(sym.GROUPBY,yyline,yychar,new TokenValue(yytext()));
}
   

<YYINITIAL,NEGATE> DISTINCT {
  yybegin(YYINITIAL);
  return new Symbol(sym.DISTINCT,yyline,yychar,new TokenValue(yytext()));
}
   
<YYINITIAL,NEGATE> "*" {
    yybegin(YYINITIAL);
    return new Symbol(sym.STAR,yyline,yychar,new TokenValue(yytext()));
}


<YYINITIAL,NEGATE> {WHITE_SPACE} { 
}


<YYINITIAL,NEGATE> {ID} { 
  yybegin(YYINITIAL);
  return new Symbol(sym.ID,yyline,yychar,new TokenValue(yytext())); 
}


<YYINITIAL,NEGATE> {STRING_LITERAL} { 
  yybegin(YYINITIAL); 
    return new Symbol(sym.STRINGLIT,yyline,yychar, new TokenValue(yytext().substring(1,yytext().length()-1))); 
  }



<YYINITIAL,NEGATE> "," {
  yybegin(NEGATE); 
  return new Symbol(sym.COMMA, yyline,yychar,new TokenValue(yytext())); 
}

<YYINITIAL,NEGATE> "=" {
  yybegin(NEGATE); 
  return new Symbol(sym.EQUAL, yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> ";" {
  yybegin(YYINITIAL); 
  return new Symbol(sym.SEMI, yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> "<" { 
  yybegin(NEGATE);
  return new Symbol(sym.LESSTHAN,yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> ">" { 
  yybegin(NEGATE);
  return new Symbol(sym.GREATERTHAN,yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> "<=" { 
  yybegin(NEGATE);
  return new Symbol(sym.LTOE,yyline,yychar,new TokenValue(yytext()));
}
<YYINITIAL,NEGATE> ">=" { 
  yybegin(NEGATE);
  return new Symbol(sym.GTOE, yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> "!=" { 
  yybegin(NEGATE);
  return new Symbol(sym.NOTEQUAL, yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> "&&" { 
  yybegin(NEGATE);
  return new Symbol(sym.AND, yyline,yychar,new TokenValue(yytext()));
}
<YYINITIAL,NEGATE> "||" { 
  yybegin(NEGATE);
  return new Symbol(sym.OR,yyline,yychar,new TokenValue(yytext()));
}

<YYINITIAL,NEGATE> "." {
  yybegin(YYINITIAL);
   return new Symbol(sym.DOT,yyline,yychar,new TokenValue(yytext()));
  
}














