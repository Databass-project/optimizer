package qp.operators;


import java.io.EOFException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Vector;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Sorter;
import qp.utils.Tuple;

public final class SortMerge extends Join {
	
	int lbatchsize;		// Number of left tuples per batch	
	int rbatchsize;		// Number of right tuples per batch	
	int jbatchsize;		// Number of joined tuples per batch
	
    int leftindex;     	// Index of the join attribute in left table
    int rightindex;		// Index of the join attribute in right table

    String lfname;		// The file name where the left sorted table is materialized
    String rfname;		// The file name where the right sorted table is materialized

    int lcurs;			// Cursor for left side tuple
    int rcurs;			// Cursor for right side tuple
    int batchcurs;		// Cursor for right side buffers
    int numReadLeft;	// Number of left memory pages read
    int numReadRight;	// Number of right memory pages read
    int batchesRead;	// Number of pages to read
    
    ObjectInputStream sortedLeft; 
    ObjectInputStream sortedRight; 
    
    Batch outBatch;
    Batch leftBatch; 
    Batch[] rightBatches; 
    int[] batchSizes; 
    
    boolean eosl;  // Whether end of stream (left table) is reached 
    boolean eosr;  // Whether end of stream (right table) is reached
    
    /* PUBLIC INTERFACE */ 

    public SortMerge(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }
   
    public boolean open() {
        /* select number of tuples per outbatch **/
        int jtuplesize = schema.getTupleSize();
        jbatchsize = Batch.getPageSize() / jtuplesize;
        
        int ltuplesize = left.schema.getTupleSize();
        lbatchsize = Batch.getPageSize() / ltuplesize;
        int rtuplesize = right.schema.getTupleSize();
        rbatchsize = Batch.getPageSize() / rtuplesize;

        getJoinAttrIndex();

        /* initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        batchcurs = 0;
        
        /* initialize the number of pages read from input streams **/
        numReadLeft = 0;
        numReadRight = 0;
        batchesRead = 0; 
        
        /* Prepare to process the streams  */
        eosl = false;
        eosr = false;
        
        Sorter lSorter = new Sorter(left, numBuff, lbatchsize, (t1,t2) -> Tuple.compareTuples(t1,t2,leftindex));
		if (lSorter.sortedFile()) {
			
			Sorter rSorter = new Sorter(right, numBuff, rbatchsize, (t1,t2) -> Tuple.compareTuples(t1,t2,rightindex));
			if (rSorter.sortedFile()) {
				
				try {
					lfname = lSorter.getSortedName();
					sortedLeft = new ObjectInputStream(new FileInputStream(lfname));
					
					rfname = rSorter.getSortedName();
					sortedRight = new ObjectInputStream(new FileInputStream(rfname));
					
					return true;
				} catch(IOException io) {
					System.out.println("SortMerge: file opening error");
					return false;
				}
				
			} else {
				return false;
			}
		} else {
			return false;
		}
    }
    
    public Batch next() { // properly close input stream
    	if (eosl || (eosr && (batchesRead == 0))) { // not entirely correct, change later
            close();
            return null;
        }
        
        outBatch = new Batch(jbatchsize); // This one is OK

        try {
        	if (numReadLeft == 0 && numReadRight == 0) {
        		leftBatch = new Batch(lbatchsize); // Problem with this -> reload data
                rightBatches = new Batch[numBuff-2]; // Problem with this -> reload data
                batchSizes = new int[numBuff-2]; // Problem with this -> reload data
                updateLeftBatch();
        		updateRightBatches();
        	}
        	
        	Tuple lefttuple;
        	Tuple righttuple;
        	int compareTuples;
	        while (!outBatch.isFull()) {
	        	do {
	        		lefttuple = leftBatch.elementAt(lcurs);
                    righttuple = rightBatches[batchcurs].elementAt(rcurs);
                    compareTuples = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                    if (compareTuples > 0) {
                    	if (rcurs < (batchSizes[batchcurs]-1)) {
                    		rcurs += 1;
                    	} else if (batchcurs < (batchesRead-1)) {
                    		rcurs = 0;
                    		batchcurs += 1;
                    	} else { // Load into memory
                    		if (eosr) {
                    			if (outBatch.isEmpty()) {
                    				return null;
                    			} else {
                    				return outBatch;
                    			}
                    		} else {
                    			updateRightBatches();
                    			rcurs = 0;
                    			batchcurs = 0;
                    		}
                    	}
                    } else if (compareTuples < 0) {
                    	if (lcurs < (leftBatch.size()-1)) {
                    		lcurs += 1;
                    	} else { // Load into memory
                    		if (eosl) {
                    			if (outBatch.isEmpty()) {
                    				return null;
                    			} else {
                    				return outBatch;
                    			}
                    		} else {
                    			updateLeftBatch();
                    			lcurs = 0;
                    		}
                    	}
                    }
	        	} while(compareTuples != 0);
	        	
	        	outBatch.add(lefttuple.joinWith(righttuple));
	        	
	        	if (rcurs < (batchSizes[batchcurs]-1)) {
            		rcurs += 1;
            	} else if (batchcurs < (batchesRead-1)) {
            		rcurs = 0;
            		batchcurs += 1;
            	} else { // Load into memory
            		if (eosr) {
            			eosl = true;
            			return outBatch;
            		} else {
            			updateRightBatches();
            			rcurs = 0;
            			batchcurs = 0;
            		}
            	}
	        }
        } catch (IOException io) {
            System.out.println("SortMerge: file operation error"); 
            System.exit(1);
        } catch (ClassNotFoundException c) {
            System.out.println("SortMerge: deserialization error");
            System.exit(1);
        }
        
        return outBatch;
    }
    
    // Delete temporary files
    public boolean close() { 
		File f = new File(lfname);
	    f.delete();
		f = new File(rfname);
	    f.delete();

	    return true;
    }
    
    
    /* PRIVATE METHODS */ 
    
    
    private void getJoinAttrIndex() {
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
    }
    
    private void updateLeftBatch() throws IOException, ClassNotFoundException {
    	try {
    		leftBatch = (Batch) sortedLeft.readObject();
    		numReadLeft += 1;
    	} catch (EOFException eof) {
    		sortedLeft.close();
    		eosl = true;
    	}
    }
    
    private void updateRightBatches() throws IOException, ClassNotFoundException {
    	try {
	    	for (batchesRead = 0; batchesRead < (numBuff-2); batchesRead += 1) {
	    		rightBatches[batchesRead] = (Batch) sortedRight.readObject();
	    		batchSizes[batchesRead] = rightBatches[batchesRead].size();
	    	}
    	} catch (EOFException eof) {
    		sortedRight.close();
    		eosr = true;
    	}
    }
    
}