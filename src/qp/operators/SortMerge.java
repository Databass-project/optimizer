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
	
	private int lbatchsize;					// Number of left tuples per batch	
	private int rbatchsize;					// Number of right tuples per batch	
	private int jbatchsize;					// Number of joined tuples per batch
	
    private int leftindex;     				// Index of the join attribute in left table
    private int rightindex;					// Index of the join attribute in right table

    private String lfname;					// The file name where the left sorted table is materialized
    private String rfname;					// The file name where the right sorted table is materialized

    private int lcurs;						// Cursor for left side tuple
    private int rcurs;						// Cursor for right side tuple
    
    private int numBlocksRead; 				// Total number of blocks (numBuff-2 pages) read from right file 
    
    private boolean equalitySequence;		// Whether there is a sequence of identical tuples on the left
    private int equalityBlockIndex;			// Page index of first identical tuple
    private int equalityTupleIndex;			// Tuple Index of first identical tuple
    private Tuple lastTuple;				// Last tuple from left file
    
    private ObjectInputStream sortedLeft; 
    private ObjectInputStream sortedRight; 
    
    private Batch outBatch;
    private Batch leftBatch; 
    private Batch rightBlock;
    
    private boolean eosl;  // Whether end of stream (left table) is reached 
    private boolean eosr;  // Whether end of stream (right table) is reached
    
    /* PUBLIC INTERFACE */ 

    public SortMerge(Join jn) { // switch left and right
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
        
        /* initialize the number of tuples read from sorted right file & OTHER **/
        rightBlock = new Batch((numBuff-2)*rbatchsize);
        numBlocksRead = 0;
        
        equalitySequence = false;
        equalityBlockIndex = -1;
        equalityTupleIndex = -1;
        lastTuple = null;
        
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
    	if (eosl || (eosr && rightBlock.isEmpty())) { 
            close();
            return null;
        }
        
        outBatch = new Batch(jbatchsize); 

        try {
        	if (rightBlock.isEmpty()) {
                updateLeftBatch();
        		readRightBatches();
        	}
        	
        	Tuple lefttuple;
        	Tuple righttuple;
        	int compareTuples;
	        while (!outBatch.isFull()) {
	        	
	        	do {
	        		lefttuple = leftBatch.elementAt(lcurs);
	        		
	        		if ((lastTuple != null) && lefttuple.checkJoin(lastTuple, leftindex, leftindex)) { 
	        			seekToTuple();
	        			lastTuple = null;
	        		} 
	        		righttuple = rightBlock.elementAt(rcurs);
	        		
                    compareTuples = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                    if (compareTuples > 0) {
                    	if (rcurs < (rightBlock.size()-1)) {
                    		rcurs += 1;
                    	} else { // Load into memory
                    		if (eosr) {
                    			if (outBatch.isEmpty()) {
                    				return null;
                    			} else {
                    				return outBatch;
                    			}
                    		} else {
                    			if (readRightBatches()) {
                    				if (outBatch.isEmpty()) {
                    					return null;
                    				} else {
                    					return outBatch;
                    				}
                    			}
                    		}
                    	}
                    } else if (compareTuples < 0) {
                    	equalitySequence = false; // explain
                		lastTuple = lefttuple; // next tuple checks for equality
                    	
                    	if (lcurs < (leftBatch.size()-1)) {
                    		lcurs += 1;
                    	} else { // Load into memory
                    		updateLeftBatch();
                    		if (eosl) {
                    			if (outBatch.isEmpty()) {
                    				return null;
                    			} else {
                    				return outBatch;
                    			}
                    		} 
                    	}
                    }
	        	} while(compareTuples != 0);
	        	
	        	outBatch.add(lefttuple.joinWith(righttuple));
	        	
	        	if (!equalitySequence) {
	        		equalityBlockIndex = numBlocksRead;
	        		equalityTupleIndex = rcurs;
	        		equalitySequence = true;
	        	} 
	        	
	        	if (rcurs < (rightBlock.size()-1)) {
            		rcurs += 1;
            	} else if (!eosr){ // Load into memory
            		if(readRightBatches()) { // update lcurs
            			if (lcurs < (leftBatch.size()-1)) {
                    		lcurs += 1;
                    	} else { // Load into memory
                    		updateLeftBatch();
                    		if (eosl) {
                    			if (outBatch.isEmpty()) {
                    				return null;
                    			} else {
                    				return outBatch;
                    			}
                    		} 
                    	}
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
    		lcurs = 0;
    		leftBatch = (Batch) sortedLeft.readObject();
    	} catch (EOFException eof) {
    		sortedLeft.close();
    		eosl = true;
    	}
    }
    
    private boolean readRightBatches() throws IOException, ClassNotFoundException {
    	rightBlock = new Batch((numBuff-2)*rbatchsize);
    	rcurs = 0;
    	try {
    		numBlocksRead += 1;
	    	while(!rightBlock.isFull()) {
	    		Batch nextBatch = (Batch) sortedRight.readObject();
	    		for(Tuple nextTuple: nextBatch.getTuples()) { 
                	rightBlock.add(nextTuple);
                }
	    	}
    	} catch (EOFException eof) {
    		sortedRight.close();
    		eosr = true;
    	}
    	return rightBlock.isEmpty();
    }
    
    private void seekToTuple() throws IOException, ClassNotFoundException {
    	if (numBlocksRead != equalityBlockIndex) {
			sortedRight.close();
			sortedRight = new ObjectInputStream(new FileInputStream(rfname));
			numBlocksRead = 0;
			for (int blockIndex = 0; blockIndex < equalityBlockIndex; blockIndex++) {
				updateRightBatches();
			}
		}
		rcurs = equalityTupleIndex;
    }
    
}