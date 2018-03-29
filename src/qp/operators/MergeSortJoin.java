package qp.operators;

import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

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

    int lnumPages; // Number of memory pages of left input stream
    int rnumPages; // Number of memory pages of right input stream
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
        
        /* initialize the number of pages of input streams **/
        lnumPages = 0;
        rnumPages = 0;

        /* initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        
        /* Prepare to process the streams  */
        eosl = false;
        eosr = false;

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
                lnumPages += 1;
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
                rnumPages += 1;
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
    
    /**
     * creates leftBatch whose size is at most numBuffers.
     * @return batch representing next block. Null if no batch was read.
     */
    public Batch fetchNextBlock(ObjectInputStream in, int numPages) { 
        try {
        	ArrayList<Batch> batches = new ArrayList<>();
		    for (int i = 0; i < numPages; i++) {    
		        Batch next = (Batch) in.readObject(); 
		        batches.add(next);
		    }
		    if (batches.size() == 0) {
		    	return null;
		    }
		    Batch nextBlock = new Batch(numPages*batchsize);
		    for (Batch b: batches) {
		    	for(int i = 0; i < b.size(); i++) {
		    		nextBlock.add(b.elementAt(i));
		    	}
		    }
		    return nextBlock;
        } catch (ClassNotFoundException c) {
            System.out.println("MergeSortJoin: Some error in deserialization");
            return null;
        } catch (IOException io) {
            System.out.println("MergeSortJoin: Temporary file reading error");
            return null;
        }
    }
    
    private int compareTuples(Tuple left, Tuple right) { 
    	int tuplesize = schema.getTupleSize();
		for(int index = 0; index < tuplesize; index++) {
			int compareAtIndex = Tuple.compareTuples(left, right, index);
			if(compareAtIndex != 0) {
				return compareAtIndex;
			}
		}
		return 0;
	}
    
    private void sortBatch(Batch b) {
    	
    	Vector<Tuple> tuples = new Vector<>(b.size());
    	for(int i = 0; i < b.size(); i++) {
    		tuples.add(b.elementAt(i));
    	}
    	
    	Collections.sort(tuples, (t1,t2) -> compareTuples(t1,t2));
    	
    	b.clear();
    	for(int i = 0; i < tuples.size(); i++) {
    		b.insertElementAt(tuples.get(i), i);
    	}
    }
    
    private boolean createSortedRuns(String filename, int numPages) {
    	try {
    		ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
    		filenum++;
            String tmpfname = "NJtemp-" + String.valueOf(filenum);
    		ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
    		
    		int leftToRead = numPages;
    		int numRead;
    		Batch nextBlock;
    		while (leftToRead > 0) {
    			numRead = (leftToRead < numBuff)? leftToRead : numBuff;
    			nextBlock = fetchNextBlock(in,numRead);
    			sortBatch(nextBlock);
    			
    			Batch nextBatch = new Batch(batchsize);
    			nextBatch.add(nextBlock.elementAt(0)); // should be safe
    			for (int i = 1; i < nextBlock.size(); i++) {
    				if (i % batchsize == 0) {
    					tmpw.writeObject(nextBatch);
    					nextBatch.clear();	
    				}
    				nextBatch.add(nextBlock.elementAt(i));
    			}
    			
    			leftToRead -= numRead;
    		}
        	in.close();
        	tmpw.close();
        	
        	ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
        	ObjectInputStream tmpr = new ObjectInputStream(new FileInputStream(tmpfname));
        	
        	for (int i = 0; i < numPages; i++) {
        		out.writeObject(tmpr.readObject());
        	}
        	
        	out.close();
        	tmpr.close();
        	
        	return true;
    	} catch (IOException io){
    		System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
    	} catch (ClassNotFoundException c) {
	        System.out.println("MergeSortJoin: Some error in deserialization ");
	        return false;
	    }
    }
    
    // got it wrong, when any Batch is empty, load next one from memory
    private ArrayList<Batch> mergeSortedRuns(ArrayList<Batch> sortedRuns) {
    	outbatch.clear();
    	ArrayList<Batch> output = new ArrayList<>();
    	ArrayList<Tuple> headTuples = new ArrayList<>();
    	for (int i = 0; i < sortedRuns.size(); i++) {
    		headTuples.add(sortedRuns.get(i).elementAt(0));
    		sortedRuns.get(i).remove(0);
    	}
    	
    	while(sortedRuns.size() > 0) {
    		if (outbatch.isFull()) {
    			// write outbatch and clear
    			output.add(outbatch); // write directly to file?
    			outbatch.clear();
    		}
    		
    	}
    	return output;
    }
    
    private boolean mergePhase(String filename, int nbrRuns) {
    	// Problem with having contiguous Buffer (instead of B-1) -> no direct access to sub-buffers 
    	outbatch = new Batch(batchsize);
    	try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
    		
    		outbatch = new Batch(batchsize);
    		
    		int runsToMerge = nbrRuns;
    		int startingSize = numBuff;
    		while(runsToMerge > 1) {
    			for(int i = 0; i < (numBuff-1); i++) { // Math.pow (numBuff-1), jth iteration, starts at 1
    				Batch nextBatch = (Batch) in.readObject();
    				
    			}
    		}
    		
    	} catch (IOException io){
    		System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
    	} catch (ClassNotFoundException c) {
	        System.out.println("MergeSortJoin: Some error in deserialization ");
	        return false;
	    }
    	
    	return true;
    }
    
    private boolean mergeSort(String filename) {
    	return true;
    }
    
    
    public Batch next() {
    	return null;
    }
    
    
}
