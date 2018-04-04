package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Vector;

import qp.utils.*;

public class OrderBy extends Operator{

    private Operator base;  // base operator
	private Vector attrSet; // List of attributes for this orderby operation
    private int[] attrIndices; // List that will contain the list of indices of the attributes in the schema, useful for sorting
    
	private int batchSize;  // Number of tuples in outBatch
    private int numBuff;  // Number of buffers available for the sorting operation
    private Batch outBatch; // Output buffer
    
    private String fName; // Name of the file that will contain the sorted tuples from the base operator
    private ObjectInputStream in; // Sorted base file being scanned
    
    private boolean eosb; /* Represents whether or not we reached the end of the base file, which will contain  all tuples
                             from the base but sorted according to the attributes in attrSet */
    
    /* PUBLIC INTERFACE */

    public OrderBy(Operator base, Vector as, int type) {
		super(type);
		this.base = base;
		this.attrSet = as;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
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
     ** Also figures out which columns are to be
     ** projected from the base operator
     **/
    public boolean open(){
    	/** num of tuples per batch**/
		int tuplesize = schema.getTupleSize();
		batchSize = Batch.getPageSize()/tuplesize;
		Schema baseSchema = base.getSchema();
		
		//
		attrIndices = new int[attrSet.size()];
		for (int i = 0; i < attrSet.size(); i++){
		    Attribute attr = (Attribute) attrSet.elementAt(i);
	  	    int index = baseSchema.indexOf(attr);
		    attrIndices[i] = index;
		}
		
		// The base is sorted according to the attributes in attrSet for this orderby operation
		Sorter sorter = new Sorter(base, numBuff, batchSize, (t1,t2) -> Tuple.compareTuplesWith(t1, t2, attrIndices));
		if(sorter.sortedFile()) {
			try {
				fName = sorter.getSortedName();
				in = new ObjectInputStream(new FileInputStream(fName));
				eosb = false;
				return true;
			} catch (IOException e) {
				System.out.print("OrderBy: file opening error");
				return false;
			}
		} else {
			return false;
		}
    }

    /** Read next page from ordered relation */
    public Batch next(){
    	
    	// if we reached the end, no more tuples are returned
	    if (eosb) {
	        close();
	        return null;
	    }	
	    
	    // read and return a batch of tuples from the sorted base
		outBatch = new Batch(batchSize);
		try {
			outBatch = (Batch) in.readObject();
		} catch (EOFException e) {
	        try {
	            in.close();
	        } catch (IOException io) {
	            System.out.println("OrderBy: file closing error");
	        }
	        eosb = true;
	    } catch (ClassNotFoundException c) {
	        System.out.println("OrderBy: deserialization error");
	        System.exit(1);
	    } catch (IOException io) {
	        System.out.println("OrderBy: file reading error");
	        System.exit(1);
	    }
	
		return outBatch;
    }

    public boolean close() {
    	try {
			in.close();
			File f = new File(fName);
    	    f.delete();
		    return true;
		} catch (IOException e) {
			System.out.println("OrderBy: file closing error");
			return false;
		}
    }
}
