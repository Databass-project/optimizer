package qp.parser;

public class TokenValue{
  public String text;

  public TokenValue() {
  }

    public TokenValue(String text) {
	this.text = text;
    }

    public String text(){
	return text;
    }

  public Boolean toBoolean() {
    return Boolean.valueOf(text);
  }

  public Character toCharacter() {
    return new Character(text.charAt(0));
  }

  public Integer toInteger() {
    if (text.startsWith("0x")) {
    	return new Integer((int)Long.parseLong(text.substring(2),16));
    } else {
    	return Integer.valueOf(text,10);
    }
  }
}
