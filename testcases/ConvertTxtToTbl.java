import java.io.*;
import java.util.*;
import qp.utils.*;
/*
  assume that the first line of the file contain the names of the
  attributes of the relation. each subsequent line represents 1
  tuple of the relation. also assume that the fields of each line
  is delimited by tabs ("\t")
*/

public class ConvertTxtToTbl {

  public static void main(String[] args) throws IOException {
    // check the arguments
    if (args.length != 1) {
      System.out.println("usage: java ConvertTxtToTbl <tablename> \n creats <tablename>.tbl files");
      System.exit(1);
    }
    String tblname= args[0];
    String mdfile = tblname+".md";
    String tblfile = tblname+".tbl";


	/** open the input and output streams **/
    BufferedReader in = new BufferedReader(new FileReader(tblname+".txt"));
    //ObjectOutputStream outmd = new ObjectOutputStream(new FileOutputStream(mdfile));
    ObjectOutputStream outtbl = new ObjectOutputStream(new FileOutputStream(tblfile));

    /** First Line is METADATA **/
    int linenum=0;
    String line;
    /**
    String line = in.readLine();
    linenum++;
    StringTokenizer tokenizer = new StringTokenizer(line);
    int count = tokenizer.countTokens();
    if(count!=1){
	System.err.println("The first line shuld be 'METADATA'");
	System.exit(1);
    }
    String title = tokenizer.nextToken();
    if(!title.equals( "METADATA")){
	System.err.println("ConvertTxtToTbl-line40: The first line shuld be 'METADATA'");
	System.exit(1);
    }

	 //Reading the meta data from text file and creating
	 // Schema of the table in Object format


	boolean flag=false;
	Vector attrlist = new Vector();

	while(flag==false){
	    line = in.readLine();
	    linenum++;
	    tokenizer = new StringTokenizer(line);
	    if(tokenizer.countTokens()==1){
		flag=true;
	    }else{
		Attribute attr;
		String attname= tokenizer.nextToken();
		String datatype = tokenizer.nextToken();
		int type;
		int keytyp;
		if(datatype.equals("INT")){
		    type= Attribute.INT;
		    //  System.out.println("integer");
		}else if(datatype.equals("STRING")){
		    type=Attribute.STRING;
		    // System.out.println("String");
		}else if(datatype.equals("REAL")){
		    type=Attribute.REAL;
		}else{
		    type=-1;
		    System.err.println("invalid data type");
		    System.exit(1);
		}
		if(tokenizer.hasMoreElements()){
		    String keytype = tokenizer.nextToken();
		    if(keytype.equals("PK"))
			keytyp=Attribute.PK;
		    else if(keytype.equals("FK"))
			keytyp=Attribute.FK;
		    else{
			keytyp=-1;
			System.err.println("invalid key type");
			System.exit(1);
		    }
		    attr = new Attribute(tblname,attname,type,keytyp);
		}else{
		    attr = new Attribute(tblname,attname,type);
		}
		attrlist.add(attr);
	    }
	}
	Schema schema = new Schema(attrlist);
	outmd.writeObject(schema);
	outmd.close();


	//  Starting of the data section

	String header = tokenizer.nextToken();
	if(header=="DATA"){
	    System.err.println("The data section should start with 'DATA'");
	    System.exit(1);
	}
	int numCols = schema.getNumCols();

	**/
	Schema schema=null;
try{
   ObjectInputStream ins = new ObjectInputStream(new FileInputStream(mdfile));
	 schema= (Schema) ins.readObject();
}catch(ClassNotFoundException ce){
	System.out.println("class not found exception --- error in schema object file");
	System.exit(1);
}

	boolean flag=false;
	StringTokenizer tokenizer;
	while((line = in.readLine()) != null){
	    linenum++;
	    tokenizer = new StringTokenizer(line);
	    //int tokencount = tokenizer.countTokens();
	    //System.out.println("numtokens= "+tokenizer.countTokens()+"numcols="+numCols);
	    //if(tokencount != numCols){
		//System.err.println("Incomplete tuple: linenum= "+linenum);
		//System.exit(1);
	    //}

	    Vector data = new Vector();
	    int attrIndex=0;

	    while(tokenizer.hasMoreElements()){
		String dataElement = tokenizer.nextToken();
		int datatype = schema.typeOf(attrIndex);
		//System.out.print("Convert :"+ dataElement+"  "+datatype);
		if(datatype==Attribute.INT){
		    //System.out.println("Integer data:"+dataElement);
		    data.add(Integer.valueOf(dataElement));
		}else if(datatype==Attribute.REAL){
		    data.add(Float.valueOf(dataElement));
		}else if(datatype==Attribute.STRING){
		    data.add(dataElement);
		}else{
		    System.err.println("Invalid data type");
		    System.exit(1);
		}
		attrIndex++;
	    }
	    Tuple tuple = new Tuple(data);
	    outtbl.writeObject(tuple);
	}
	outtbl.close();

	in.close();
  }
}




































