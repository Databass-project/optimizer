/** Attibute or column meta data **/


package qp.utils;
import java.io.Serializable;

public class Attribute implements Serializable{

	/** enumerating type of attribute **/
    public static final int INT=1;
    public static final int STRING=2;
    public static final int REAL=3;

	/** enumeration of type of key **/

    public static final int PK=1;   // primary key
    public static final int FK=2;   // Foreign key


    String tblname;        //tabel to which this attribute belongs
    String colname;         //name of the attribute **/
    int type;  // whether integer or real or string
    int key=-1;   // type of the key
    int attrsize;   // Number of bytes for this attribute


    public Attribute(String tbl, String col){
	tblname=tbl;
	colname=col;
    }

    public Attribute(String tbl, String col, int typ){
	tblname = tbl;
	colname = col;
	type = typ;
    }

    public Attribute(String tbl, String col, int typ,int keytype){
	tblname = tbl;
	colname = col;
	type = typ;
	key = keytype;
    }

	public Attribute(String tbl, String col, int typ,int keytype,int size){
		tblname = tbl;
		colname = col;
		type = typ;
		key = keytype;
		attrsize=size;
    }

	public void setAttrSize(int size){
		attrsize=size;
    }

    public int getAttrSize(){
		return attrsize;
    }

    public void setKeyType(int kt){
	key =kt;
    }

    public int getKeyType(){
	return key;
    }

    public boolean isPrimaryKey(){
	if(key==PK)
	    return true;
	else
	    return false;
    }

    public boolean isForeignKey(){
	if(key==FK)
	    return true;
	else
	    return false;
    }


    public void setTabName(String tab){
	tblname = tab;
    }

    public String getTabName(){
	return tblname;
    }

    public void setColName(String col){
	colname = col;
    }

    public String getColName(){
	return colname;
    }

    public void setType(int typ){
	type=typ;
    }

    public int getType(){
	return type;
    }

    public boolean equals(Attribute attr){
	if(this.tblname.equals(attr.getTabName()) && this.colname.equals(attr.getColName()))
	    return true;
	else
	    return false;
    }

    public Object clone(){
	String newtbl =  tblname;
	String newcol =  colname;
	Attribute newattr = new Attribute(newtbl,newcol);
	newattr.setType(type);
	newattr.setKeyType(key);
	newattr.setAttrSize(attrsize);
	return newattr;
    }

}





