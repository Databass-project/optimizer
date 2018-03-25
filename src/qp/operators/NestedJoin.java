/** page nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class NestedJoin extends Join{


	int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)

    public NestedJoin(Join jn){
	super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/



    public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

	Attribute leftattr = con.getLhs();
	Attribute rightattr =(Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);
	Batch rightpage;
	/** initialize the cursors of input buffers **/

	lcurs = 0; rcurs =0;
	eosl=false;
	/** because right stream is to be repetitively scanned
	 ** if it reached end, we have to start new scan
	 **/
	eosr=true;

	/** Right hand side table is to be materialized
	 ** for the Nested join to perform
	 **/

	if(!right.open()){
	    return false;
	}else{
	    /** If the right operator is not a base table then
	     ** Materialize the intermediate result from right
	     ** into a file
	     **/

	    //if(right.getOpType() != OpType.SCAN){
	    filenum++;
	    rfname = "NJtemp-" + String.valueOf(filenum);
	    try{
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
		while( (rightpage = right.next()) != null){
		    out.writeObject(rightpage);
		}
		out.close();
	    }catch(IOException io){
		System.out.println("NestedJoin:writing the temporay file error");
		return false;
	    }
		//}
	    if(!right.close())
		return false;
	}
	if(left.open())
	    return true;
	else
	    return false;
    }



    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
	//System.out.print("NestedJoin:--------------------------in next----------------");
	//Debug.PPrint(con);
	//System.out.println();
	int i,j;
	if(eosl){
		close();
	    return null;
	}
	outbatch = new Batch(batchsize);


	while(!outbatch.isFull()){

	    if(lcurs==0 && eosr==true){
		/** new left page is to be fetched**/
		leftbatch =(Batch) left.next();
		if(leftbatch==null){
		    eosl=true;
		    return outbatch;
		}
		/** Whenver a new left page came , we have to start the
		 ** scanning of right table
		 **/
		try{

		    in = new ObjectInputStream(new FileInputStream(rfname));
		    eosr=false;
		}catch(IOException io){
		    System.err.println("NestedJoin:error in reading the file");
		    System.exit(1);
		}

	    }

	    while(eosr==false){

		try{
		    if(rcurs==0 && lcurs==0){
			rightbatch = (Batch) in.readObject();
		    }

		    for(i=lcurs;i<leftbatch.size();i++){
			for(j=rcurs;j<rightbatch.size();j++){
			    Tuple lefttuple = leftbatch.elementAt(i);
			    Tuple righttuple = rightbatch.elementAt(j);
			    if(lefttuple.checkJoin(righttuple,leftindex,rightindex)){
				Tuple outtuple = lefttuple.joinWith(righttuple);

				//Debug.PPrint(outtuple);
				//System.out.println();
				outbatch.add(outtuple);
				if(outbatch.isFull()){
				    if(i==leftbatch.size()-1 && j==rightbatch.size()-1){//case 1
					lcurs=0;
					rcurs=0;
				    }else if(i!=leftbatch.size()-1 && j==rightbatch.size()-1){//case 2
					lcurs = i+1;
					rcurs = 0;
				    }else if(i==leftbatch.size()-1 && j!=rightbatch.size()-1){//case 3
					lcurs = i;
					rcurs = j+1;
				    }else{
					lcurs = i;
					rcurs =j+1;
				    }
				    return outbatch;
				}
			    }
			}
			rcurs =0;
		    }
		    lcurs=0;
		}catch(EOFException e){
		    try{
			in.close();
		    }catch (IOException io){
			System.out.println("NestedJoin:Error in temporary file reading");
		    }
		    eosr=true;
		}catch(ClassNotFoundException c){
		    System.out.println("NestedJoin:Some error in deserialization ");
		    System.exit(1);
		}catch(IOException io){
		    System.out.println("NestedJoin:temporary file reading error");
		    System.exit(1);
		}
	    }
	}
	return outbatch;
    }



    /** Close the operator */
    public boolean close(){

	File f = new File(rfname);
	f.delete();
	return true;

    }


}











































