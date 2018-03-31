package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
    LinkedList<String> runfnames; // File name of the sorted run files that need to be merged
    int finalNumBatches;

    static int filenum = 0;   // To get unique filenum for this operation

    int lnumPages; // Number of memory pages of left input stream
    int rnumPages; // Number of memory pages of right input stream
    int lcurs; // Cursor for left side tuple
    int rcurs; // Cursor for right side tuple
    int batchcurs; // Cursor for right side buffers
    int numReadLeft; // Number of left memory pages read
    int numReadRight; // Number of right memory pages read
    int numToRead; // Number of right pages to read
    
    ObjectInputStream sortedLeft; 
    ObjectInputStream sortedRight; 
    
    Batch outBatch;
    Batch leftBatch; // Problem with this -> double mem space?
    Batch[] rightBatches; // Problem with this -> double mem space?
    int[] batchSizes; 
    
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
        batchcurs = 0;
        
        /* initialize the number of pages read from input streams **/
        numReadLeft = 0;
        numReadRight = 0;
        numToRead = 0; // change val
        
        /* Prepare to process the streams  */
        eosl = false;
        eosr = false;
        
        boolean materialized = materializeLeftTable() && materializeRightTable();
        
        if(materialized) {
	        try {
	        	boolean sorted = mergeSort(lfname,lnumPages,leftindex);
	            sortedLeft = new ObjectInputStream(new FileInputStream(runfnames.removeFirst()));
	            sorted = sorted && mergeSort(rfname,rnumPages,rightindex);
	            sortedRight = new ObjectInputStream(new FileInputStream(runfnames.removeFirst()));
	            
	            return sorted; 
	        } catch (IOException io) {
	            System.out.println("MergeSortJoin: temporary file reading error");
	            return false;
	        }
        } else {
        	return false;
        }
    }
    
    private String temporaryFileName() {
    	filenum++;
        String filename = "MSJtemp-" + String.valueOf(filenum);
    	return filename;
    }
    
    /**
     * @return true if left table is successfully written into a file. false otherwise
     */
    private boolean materializeLeftTable() {
        if (!left.open()) {
            return false;
        }

        lfname = temporaryFileName();
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname))) {
        	Batch leftpage;
            while ((leftpage = left.next()) != null) {
                out.writeObject(leftpage);
                lnumPages += 1;
            }
        } catch (IOException io) {
            System.out.println("MergeSortJoin: writing the temporary file error");
            return false;
        }
        if (!left.close())
            return false;

        return true;
    }

    /**
     * @return true if right table is successfully written into a file. false otherwise
     */
    private boolean materializeRightTable() {
        if (!right.open()) {
            return false;
        }

        rfname = temporaryFileName();
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname))) {
        	Batch rightpage;
            while ((rightpage = right.next()) != null) {
                out.writeObject(rightpage);
                rnumPages += 1;
            }
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
    
    private Batch fetchNextBlock(ObjectInputStream in, int numBatches) throws IOException, ClassNotFoundException { 
    	Batch nextBlock = new Batch(numBatches*batchsize); // Our B memory buffers
	    for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {    
	        Batch nextBatch = (Batch) in.readObject(); 
	        
	        for (int tupleIndex = 0; tupleIndex < nextBatch.size(); tupleIndex++) {
	    		nextBlock.add(nextBatch.elementAt(tupleIndex));
	    	}
	    }
	    return nextBlock;
    }
    
    private void writeBlockToFile(ObjectOutputStream out, Batch nextBlock) throws IOException {
    	Batch nextBatch = new Batch(batchsize);
		nextBatch.add(nextBlock.elementAt(0)); // should be safe
		
		for (int i = 1; i < nextBlock.size(); i++) {
			if (i % batchsize == 0) {
				out.writeObject(nextBatch); 
				nextBatch = new Batch(batchsize); // necessary because clear acts on object	
			}
			nextBatch.add(nextBlock.elementAt(i));
		}
		out.writeObject(nextBatch); // add last batch
    }
    
    /** Modify: sort using join attributes
    private int compareTuples(Tuple left, Tuple right) {  // sort using join attributes
    	int numAttr = left._data.size(); // tuples have the same size, and same attributes
		for (int index = 0; index < numAttr; index++) { 
			int compareAtIndex = Tuple.compareTuples(left, right, index);
			if (compareAtIndex != 0) {
				return compareAtIndex;
			}
		}
		return 0;
	}**/
    
    private void createSortedRuns(String filename, int numBatches, int attrIndex) throws IOException, ClassNotFoundException {
    	runfnames = new LinkedList<>();
    	ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
		
		int leftToRead = numBatches;
		while (leftToRead > 0) {
			int numRead = numBuff;
			if (leftToRead <= numBuff) {
				numRead = leftToRead;
				finalNumBatches = leftToRead;
			}
			Batch nextBlock = fetchNextBlock(in,numRead);
			Collections.sort(nextBlock.getTuples(), (t1,t2) -> Tuple.compareTuples(t1, t2, attrIndex));
			
			// Creates a new file for a sorted run
			String tmpfname = temporaryFileName();
			ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
			writeBlockToFile(tmpw,nextBlock);
			tmpw.close();
			runfnames.add(tmpfname); // explain
			
			leftToRead -= numRead;
		}
		
    	in.close();
    }
    
    private int writeRunsToMemory(Vector<ObjectInputStream> runFiles, Vector<String> fnames, HashMap<String,Integer> batchesPerRun, Batch[] inBatches, int runSize, boolean lastRun) 
    		throws IOException, ClassNotFoundException {
    	
    	int numMerge = runFiles.capacity();
    	int numBatchesToMerge = 0;
    	
    	for(int runIndex = 0; runIndex < numMerge; runIndex++) {
			String fname = runfnames.remove();
			fnames.add(fname);
			ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(fname));
			inBatches[runIndex] = ((Batch) nextStream.readObject());
			runFiles.add(nextStream);
			
			int numBatches = (lastRun && (runIndex == (numMerge-1)))? finalNumBatches : runSize;
			batchesPerRun.put(fname, numBatches);
			numBatchesToMerge += numBatches;
		}
   
    	runfnames.add(fnames.get(0)); // explain
    	
    	return numBatchesToMerge;
    }
    
    private int minTupleIndex(Batch[] inBatches, int attrIndex) {
    	Tuple minTuple = null;
    	int minTupleIndex = -1;
    	
    	for (int batchIndex = 0; batchIndex < inBatches.length; batchIndex++) {
    		Batch nextBatch = inBatches[batchIndex];
    		
    		if(!nextBatch.isEmpty()) {
    			Tuple nextTuple = nextBatch.head();
    			
    			if((minTupleIndex == -1) || (Tuple.compareTuples(nextTuple, minTuple, attrIndex) < 0)) {
    				minTuple = nextTuple;
    				minTupleIndex = batchIndex;
    			} 
    		}
    	}
    	
    	return minTupleIndex;
    }
    
    private int mergeRuns(ObjectOutputStream out, Batch[] inBatches, int numMerge, int runSize, int attrIndex, boolean lastRun) 
    		throws IOException, ClassNotFoundException {
    	
    	Vector<ObjectInputStream> runFiles = new Vector<>(numMerge);
		Vector<String> fnames = new Vector<>(numMerge);
		HashMap<String,Integer> batchesPerRun = new HashMap<>();
		int numBatchesToMerge = writeRunsToMemory(runFiles, fnames, batchesPerRun, inBatches, runSize, lastRun);
		
    	for (int mergedIndex = 0; mergedIndex < numBatchesToMerge; mergedIndex++) {
			for (int tupleIndex = 0; tupleIndex < batchsize; tupleIndex++) {
				int minIndex = minTupleIndex(inBatches,attrIndex);
				if (minIndex == -1) {
					break;
				}
				
				Batch minBatch = inBatches[minIndex];
				Tuple minTuple = minBatch.head();
				outBatch.add(minTuple);
				minBatch.removeHead();
				
				if(minBatch.isEmpty()) {
					String fname = fnames.get(minIndex);
					if (batchesPerRun.get(fname) > 0) {
						inBatches[minIndex] = (Batch) runFiles.get(minIndex).readObject();
						batchesPerRun.put(fname,batchesPerRun.get(fname)-1);
					}
				}
			}
			
			out.writeObject(outBatch);
			outBatch.clear();
		}
    	
    	for (ObjectInputStream nextStream: runFiles) nextStream.close();
    	
    	return numBatchesToMerge;
    }
    
    private void mergePhase(int numBatches, int attrIndex) throws IOException, ClassNotFoundException {
    	outBatch = new Batch(batchsize); 
    	Batch[] inBatches = new Batch[numBuff-1]; // define a constant 
    	String tmpMergedName = temporaryFileName(); 
		
    	int runSize = numBuff; // number of batches in a sorted run
		while (runSize < numBatches) { 
			
			int leftToMerge = runfnames.size();
			while (leftToMerge > 1) {
				
				int numMerge = numBuff-1;
				boolean lastRun = false;
				if (leftToMerge <= (numBuff-1)) {
					numMerge = leftToMerge;
					lastRun = true;
				}
				
				ObjectOutputStream tmpMerged = new ObjectOutputStream(new FileOutputStream(tmpMergedName));
				int numBatchesMerged = mergeRuns(tmpMerged, inBatches, numMerge, runSize, attrIndex, lastRun);
				tmpMerged.close();
				
				ObjectInputStream in = new ObjectInputStream(new FileInputStream(tmpMergedName));
				String mergefname = runfnames.getLast(); // Merged-file name was appended
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(mergefname));
				for (int batchIndex = 0; batchIndex < numBatchesMerged; batchIndex++) {
					out.writeObject(in.readObject());
				}
				in.close();
				out.close();
				
				if(lastRun) finalNumBatches += (numMerge-1)*runSize; // explain
				leftToMerge -= numMerge;
				
			}
			runSize = runSize*(numBuff-1);
		} 
    }
    
    private boolean mergeSort(String filename, int numBatches, int attrIndex) {
    	try {
    		createSortedRuns(filename, numBatches, attrIndex);
    		mergePhase(numBatches,attrIndex);
    		return true;
    	} catch (IOException io) {
            System.out.println("MergeSortJoin: file RW error");
            return false;
        } catch (ClassNotFoundException c) {
            System.out.println("MergeSortJoin: Some error in deserialization");
            return false;
        } 
    }
    
    private void updateLeftBatch() throws IOException, ClassNotFoundException {
    	leftBatch = (Batch) sortedLeft.readObject();
    	numReadLeft += 1;
    	eosl = (numReadLeft >= lnumPages);
    }
    
    private void updateRightBatches() throws IOException, ClassNotFoundException {
    	numToRead = numBuff-2;
    	eosr = ((numReadRight+numToRead) >= rnumPages);
    	if (eosr) {
    		numToRead = rnumPages-numReadRight;
    	}
    	numReadRight += numToRead;
    	
    	for (int batchIndex = 0; batchIndex < numToRead; batchIndex++) {
    		rightBatches[batchIndex] = (Batch) sortedRight.readObject();
    		batchSizes[batchIndex] = rightBatches[batchIndex].size();
    	}
    }
    
    
    public Batch next() {
        if (eosl || eosr) { // not entirely correct, change later
            close();
            return null;
        }
        
        outBatch = new Batch(batchsize); // This one is OK

        try {
        	if(numReadLeft == 0 && numReadRight == 0) {
        		leftBatch = new Batch(batchsize); // Problem with this -> reload data
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
                    	if(rcurs < (batchSizes[batchcurs]-1)) {
                    		rcurs += 1;
                    	} else if (batchcurs < (numToRead-1)) {
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
                    	if (lcurs < leftBatch.size()-1) {
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
	        	
	        	if ((rcurs == (batchSizes[batchcurs]-1)) && (batchcurs == (numToRead-1))) {
	        		if (eosr) {
            			return outBatch;
            		} else {
            			updateRightBatches();
            		}
	        	} else {
	        		rcurs += 1;
	        	}
	        }
        } catch (IOException io) {
            System.out.println("MergeSortJoin temporary file reading error"); 
            System.exit(1);
        } catch (ClassNotFoundException c) {
            System.out.println("MergeSortJoin: Some error in deserialization ");
            System.exit(1);
        }
        
        return outBatch; // bad programming, will never reach
    }
    
    
    // Delete temporary files
    public boolean close() {
    	for (int fnum = 1; fnum <= filenum; fnum++) {
    		String filename = "MSJtemp-" + String.valueOf(fnum);
    		File f = new File(filename);
    	    f.delete();
    	}
	    return true;
    }
    
}
