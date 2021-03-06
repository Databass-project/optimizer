package qp.operators;


import java.io.EOFException;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
    
    private Batch outBatch;					// Output memory buffer
    private Batch leftBatch; 				// Input buffer for left file
    private Batch rightBlock;				// (numBuff-2) input buffers for right file

    private String lfname;					// The file name where the left sorted table is materialized
    private String rfname;					// The file name where the right sorted table is materialized
    
    private ObjectInputStream sortedLeft; 	// Sorted materialized left file
    private ObjectInputStream sortedRight;	// Sorted materialized right file
    
    private boolean eosl;  					// Whether end of stream (left table) is reached 
    private boolean eosr;  					// Whether end of stream (right table) is reached
    private boolean endOfJoin;				// Whether the join is done

    private int lcurs;						// Cursor for left side tuple
    private int rcurs;						// Cursor for right side tuple
    private int numBlocksRead; 				// Total number of blocks (numBuff-2 pages) read from right file 
    
    private boolean leftJoined;				// Whether lastTuple joined with a tuple  
    private int joinedBlockIndex;			// Page index of first joined Tuple
    private int joindedTupleIndex;			// Tuple Index of first joined tuple
    private Tuple lasttuple;				// Last tuple from left file 
    
    /* =============================== PUBLIC INTERFACE =============================== */

    /**
     * Creates a new Join that will perform MergeSortJoin
     * @param jn
     */
    public SortMerge(Join jn) { // switch left and right
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }
    
    /**
     * @return true if join attribute indexes are retrieved, left and right tables are materialized and sorted
     */
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
        leftBatch = new Batch(lbatchsize);
        numBlocksRead = 0;
        
        leftJoined = false;
        joinedBlockIndex = -1;
        joindedTupleIndex = -1;
        lasttuple = null;

        /* Prepare to process the streams  */
        eosl = false;
        eosr = false;
        endOfJoin = false;
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
    
    /**
     * from input buffers select the tuples satisfying join condition. And returns a page of output tuples
     */
    public Batch next() { 
    	if (endOfJoin) { 
            close();
            return null;
        }

        outBatch = new Batch(jbatchsize);

        try {
        	
        	if (leftBatch.isEmpty()) {
                nextLeftBatch();
        		nextRightBlock();
        	}
        	Tuple lefttuple;
        	Tuple righttuple;
        	int compareTuples;
        	
	        while (!outBatch.isFull()) {
	        	do {
	        		lefttuple = leftBatch.elementAt(lcurs);
	        		righttuple = rightBlock.elementAt(rcurs);
                    compareTuples = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                    if ((compareTuples > 0) && (updatercurs())) { // update rcurs
                    	if (outBatch.isEmpty()) {
                    		return null;
                   		} else {
                   			endOfJoin = true; 
                   			return outBatch;
                   		}
                    } else if ((compareTuples < 0) && updatelcurs(lefttuple)) { // update lcurs
                    	if (outBatch.isEmpty()) {
               			 	return null;
               		 	} else {
               		 		endOfJoin = true;
               		 		return outBatch;
               		 	}
                    }
	        	} while(compareTuples != 0);

	        	outBatch.add(lefttuple.joinWith(righttuple));
	        	
	        	if (!leftJoined) { // First time lefttuple joins with a right tuple
	        		joinedBlockIndex = numBlocksRead;
	        		joindedTupleIndex = rcurs;
	        		leftJoined = true;
	        	} 
	        	
	        	if (rcurs < (rightBlock.size()-1)) { // update rcurs
            		rcurs += 1;
            	} else if ((eosr || updatercurs()) && updatelcurs(lefttuple)) {
        			endOfJoin = true;
        			return outBatch;
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
    
    /**
     * @return true if files of materialized left and right tables were properly deleted
     */
    public boolean close() { 
		File f = new File(lfname);
	    f.delete();
		f = new File(rfname);
	    f.delete();

	    return true;
    }
    
    
    /* =============================== PRIVATE METHODS =============================== */ 
   
    
    private void getJoinAttrIndex() {
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
    }
    
    /**
     * updates lcurs
     * @param lefttuple, to check if going back in the right table is necessary
     * @return true if lcurs can't be updated
     */
    private boolean updatelcurs(Tuple lefttuple) throws IOException, ClassNotFoundException {
    	lasttuple = lefttuple;
    	if (lcurs < (leftBatch.size()-1)) {
    		lcurs += 1;
    	} else if (nextLeftBatch()){
    		return true;
    	}
    	
    	Tuple nextltuple = leftBatch.elementAt(lcurs);
    	
    	if (leftJoined && (Tuple.compareTuples(nextltuple, lasttuple, leftindex) == 0)) {
			seekToTuple();
		} 
    	
    	leftJoined = false;
    	
    	return false;
    }
    
    /**
     * Loads next left page into memory
     * @return true if the end of the left file was reached
     */
    private boolean nextLeftBatch() throws IOException, ClassNotFoundException {
    	try {
    		lcurs = 0;
    		leftBatch = (Batch) sortedLeft.readObject();
    	} catch (EOFException eof) {
    		sortedLeft.close();
    		eosl = true;
    	}
    	return eosl;
    }

    
    /**
     * updates rcurs
     * @return true if rcurs can't be updated
     */
    private boolean updatercurs() throws IOException, ClassNotFoundException {
    	if (rcurs < (rightBlock.size()-1)) {
    		rcurs += 1;
    		return false;
    	} else {
    		boolean ret = eosr || nextRightBlock();
    		return ret;
    	}
    }
    
    /**
     * Loads the next right block into memory
     * @return true if no more tuples were read into memory
     */
    private boolean nextRightBlock() throws IOException, ClassNotFoundException {
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
    
    /**
     * Set rcurs to joinedTupleIndex, and fetch the corresponding block
     */
    private void seekToTuple() throws IOException, ClassNotFoundException {
    	if (numBlocksRead != joinedBlockIndex) {
    		eosr = false;
			sortedRight.close();
			sortedRight = new ObjectInputStream(new FileInputStream(rfname));
			numBlocksRead = 0;
			for (int blockIndex = 1; blockIndex <= joinedBlockIndex; blockIndex++) {
				nextRightBlock();
			}
		}
		rcurs = joindedTupleIndex;
    }

}