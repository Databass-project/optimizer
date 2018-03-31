/** To projec out the required attributes from the result **/

package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;
import java.util.Set;
public class OrderBy extends Operator{

    Operator base;
    Vector attrSet;
	int batchsize;  // number of tuples per outbatch
    static int filenum = 0;   // To get unique filenum for this operation
    String bname;    // The file name where the base table is materialized
    int bnumPages; // Number of memory pages of base input stream
    int numBuff; 
    boolean eosb;  // End of stream (base table)
    ObjectInputStream in;      // Base file being scanned
    /** The following fields are requied during execution
     ** of the Project Operator
     **/

    Batch inbatch;
    Batch outbatch;

    /** index of the attributes in the base operator
     ** that are to be projected
     **/

    int[] attrIndex;


    public OrderBy(Operator base, Vector as, int type){
	super(type);
	this.base=base;
	this.attrSet=as;

    }

    /* number of buffers available to this OrderBy operator */

    public void setNumBuff(int num) {
        this.numBuff = num;
    }
    
    public int getNumBuff() {
        return numBuff;
    }
    
    public void setBase(Operator base){
	this.base = base;
    }

    public Operator getBase(){
	return base;
    }

    public Vector getOrdAttr(){
	return attrSet;
    }

    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open(){
	/** setnumber of tuples per batch **/
	int tuplesize = schema.getTupleSize();
	batchsize=Batch.getPageSize()/tuplesize;

	Schema baseSchema = base.getSchema();
	attrIndex = new int[attrSet.size()];
	//System.out.println("OrderBy---Schema: ----------in open-----------");
	//System.out.println("base Schema---------------");
	//Debug.PPrint(baseSchema);
	for(int i=0;i<attrSet.size();i++){
	    Attribute attr = (Attribute) attrSet.elementAt(i);
  	    int index = baseSchema.indexOf(attr);
	    attrIndex[i]=index;

	    //  Debug.PPrint(attr);
	    //System.out.println("  "+index+"  ");
	}
	
		boolean materialized = materializeBase();
		boolean sorted = mergesort(bname, bnumPages, attrSet);
		try {
            in = new ObjectInputStream(new FileInputStream(bname));
            eosb = false; // End of stream (base table)
        } catch (IOException io) {
            System.err.println("NestedJoin:error in reading the file");
            System.exit(1);
        }
		return materialized && sorted;
    }
    
    //TODO USE CODE FOR MERGESORT IN UTILS OR WHEREVER
    public boolean mergesort(String bname, int bnumPages, Vector attrSet){
    	return true;
    }


    /** Read next tuple from operator */

    public Batch next(){
    //System.out.println("OrderBy---Schema: ----------in next-----------");
    if (eosb) {
        close();
        return null;
    }	
    	
	outbatch = new Batch(batchsize);

	try {
		outbatch = (Batch) in.readObject();
	}catch (EOFException e) {
        try {
            in.close();
        } catch (IOException io) {
            System.out.println("OrderBy: Error in temporary file reading");
        }
        eosb = true;
    } catch (ClassNotFoundException c) {
        System.out.println("OrderBy: Some error in deserialization ");
        System.exit(1);
    } catch (IOException io) {
        System.out.println("OrderBy: temporary file reading error");
        System.exit(1);
    }
	
		return outbatch;
    }

    //TODO CODE
    /** Close the operator */
    public boolean close(){
		return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }

    
    private String temporaryFileName() {
    	filenum++;
        String filename = "OBtemp-" + String.valueOf(filenum);
    	return filename;
    }
    
    /**
     * @return true if left table is successfully written into a file. false otherwise
     */
    private boolean materializeBase() {
        if (!base.open()) {
            return false;
        }

        bname = temporaryFileName();
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(bname))) {
        	Batch basepage;
            while ((basepage = base.next()) != null) {
                out.writeObject(basepage);
                bnumPages += 1;
            }
        } catch (IOException io) {
            System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
        }
        if (!base.close())
            return false;

        return true;
    }

    // TODO code this method
    public Object clone(){
    	System.out.println("Clone is not implemented");
    	return null;
    }
}
