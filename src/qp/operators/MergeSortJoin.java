package qp.operators;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public final class MergeSortJoin extends Join {
	int batchsize;  //Number of tuples per out batch

    /* The following fields are useful during execution of the NestedJoin operation */
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String lfname;    // The file name where the left table is materialized
    String rfname;    // The file name where the right table is materialized

    static int filenum = 0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftBlock;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream lin; // File pointer to the left hand materialized file
    ObjectInputStream rin; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached 
    boolean eosr;  // End of stream (right table) -> both not really needed anymore

    public MergeSortJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }
    
    /**
     * @return true if join attribute indexes are retrieved, right table is materialized and left operator opens. false otherwise.
     */
    public boolean open() {
        /* select number of tuples per outbatch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        getJoinAttrIndex();

        /* initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;

        /* because right stream is to be repetitively scanned if it reached end, we have to start new scan */
        eosr = true;

        if (!(materializeLeftTable() && materializeRightTable())) return false;

        return true; // problem here, what to return ?
    }
    
    /**
     * @return true if left table is successfully written into a file. false otherwise
     */
    private boolean materializeLeftTable() {
        Batch leftpage;
        if (!left.open()) {
            return false;
        }

        filenum++;
        lfname = "NJtemp-" + String.valueOf(filenum);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
            while ((leftpage = left.next()) != null) {
                out.writeObject(leftpage);
            }
            out.close();
        } catch (IOException io) {
            System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
        }
        if (!right.close())
            return false;

        return true;
    }

    /**
     * @return true if right table is successfully written into a file. false otherwise
     */
    private boolean materializeRightTable() {
        Batch rightpage;
        if (!right.open()) {
            return false;
        }

        filenum++;
        rfname = "NJtemp-" + String.valueOf(filenum);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
            while ((rightpage = right.next()) != null) {
                out.writeObject(rightpage);
            }
            out.close();
        } catch (IOException io) {
            System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
        }
        if (!right.close())
            return false;

        return true;
    }
   

    private void getJoinAttrIndex() {
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
    }
    
    private int compareTuples(Tuple left, Tuple right, int tupleSize) { // get rid of tupleSize somehow (schema.getTupleSize())
		for(int index = 0; index < tupleSize; index++) {
			int compareAtIndex = Tuple.compareTuples(left, right, index);
			if(compareAtIndex != 0) {
				return compareAtIndex;
			}
		}
		return 0;
	}
    
    private void openTableFile(boolean isLeft) {
        try {
        	if(isLeft) {
        		lin = new ObjectInputStream(new FileInputStream(lfname));
                eosl = false;
        	} else {
        		rin = new ObjectInputStream(new FileInputStream(rfname))
        	}
        } catch (IOException io) {
            System.err.println("MergeSortJoin: error in reading the file");
            System.exit(1);
        }
    }
    
    /**
     * creates leftBatch whose size is at most numBuffers.
     * @return batch representing next block. Null if no batch was read.
     */
    public Batch fetchNextBlock(ObjectInputStream in, int numPages) { // is numPages really what we want -> sorted Runs
        ArrayList<Batch> nextBatches = new ArrayList<>();
        int numTuplesInLeftTable = 0;
	    for (int i = 0; i < numPages; i++) {
	    	Batch next = null;
	        try {
	        	next = (Batch) in.readObject(); // add exceptions, change scope
	        } catch (ClassNotFoundException c) {
                System.out.println("BlockNestedJoin: Some error in deserialization ");
                System.exit(1);
            }
	        if (next == null)
	            break;
	        numTuplesInLeftTable = next.size(); // this will remain constant
	        nextBatches.add(next);
	    }
        if (nextBatches.size() == 0) // no batches were added
            return null;
        Batch nextBlock = new Batch(numTuplesInLeftTable*nextBatches.size());
        for(Batch b: nextBatches) {
            for (int j = 0; j < b.size(); j++) {
                nextBlock.add(b.elementAt(j));
            }
        }
        return nextBlock;
    }
    
    private void createSortedRuns() {
    	
    }
    
    private void mergeSortedRuns() {
    	
    }
    
    private boolean mergeSort(String filename) {
    	try {
    		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
    	} catch (IOException io){
    		System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
    	}
    	
    	
    	
    	return true;
    }
    
    
    public Batch next() {
    	return null;
    }
    
    
}
