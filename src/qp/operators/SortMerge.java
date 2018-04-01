package qp.operators;


import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public final class SortMerge extends Join {
	int lbatchsize;
	int rbatchsize;
	int jbatchsize;  //Number of joined tuples per out batch

    /* The following fields are useful during execution of the NestedJoin operation */
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String lfname;    // The file name where the left table is materialized
    String rfname;    // The file name where the right table is materialized
    LinkedList<String> runfnames; // File name of the sorted run files that need to be merged

    static int filenum = 0;   // To get unique filenum for this operation

    int lnumPages; // Number of memory pages of left input stream
    int rnumPages; // Number of memory pages of right input stream
    int lcurs; // Cursor for left side tuple
    int rcurs; // Cursor for right side tuple
    int batchcurs; // Cursor for right side buffers
    int numReadLeft; // Number of left memory pages read
    int numReadRight; // Number of right memory pages read
    int numToRead; // Number of pages to read
    
    ObjectInputStream sortedLeft; 
    ObjectInputStream sortedRight; 
    
    Batch outBatch;
    Batch leftBatch; // Problem with this -> double mem space?
    Batch[] rightBatches; // Problem with this -> double mem space?
    int[] batchSizes; 
    
    boolean eosl;  // Whether end of stream (left table) is reached 
    boolean eosr;  // End of stream (right table) -> both not really needed anymore

    public SortMerge(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType(); // why?
        numBuff = jn.getNumBuff();
    }
    
    /**
     * @return true if join attribute indexes are retrieved, right table is materialized and left operator opens. false otherwise.
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
        
        //boolean materialized = materializeLeftTable() && materializeRightTable();
        
        
        try {
        	if (leftSortedRuns()) {
	        	/**int numTuples = 0;
	        	for (String fname: runfnames) {
	        		numTuples += showFileContent(fname, numBuff, leftindex);
	        	}
	        	System.out.println("Total number of tuples: " + numTuples);**/
	        	if (mergeSort(lnumPages, lbatchsize, leftindex)) {
	        		lfname = runfnames.removeFirst();
	        		int numTuples = showFileContent(lfname, lnumPages, leftindex);
	        		System.out.println("Left number of tuples: " + numTuples);
		            sortedLeft = new ObjectInputStream(new FileInputStream(lfname));
		            if (rightSortedRuns()) {
			            /**numTuples = 0;
			        	for (String fname: runfnames) {
			        		numTuples += showFileContent(fname, numBuff, rightindex);
			        	}
			        	System.out.println("Total number of tuples: " + numTuples);**/
			            if (mergeSort(rnumPages, rbatchsize, rightindex)) {
				            rfname = runfnames.removeFirst();
				            numTuples = showFileContent(rfname, rnumPages, rightindex);
			        		System.out.println("Right number of tuples: " + numTuples);
				            sortedRight = new ObjectInputStream(new FileInputStream(rfname));
				            return true;
		            	} else {
		            		return false;
		            	}
		            } else {
		            	return false; // print
		            }
	        	} else {
	        		return false; // print
	        	}
        } else {
        		return false; // print
        	}
        } catch (IOException io) {
            System.out.println("SortMerge: temporary file reading error");
            io.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
        	return false;
        }
        
    }
    
    private int showFileContent(String fname, int numPages, int attrIndex) throws ClassNotFoundException, IOException { // helper method
    	int tupleNumber = 0;
		
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(fname));
		
		for (int batchIndex = 0; batchIndex < numPages; batchIndex++) {
			
			try {
				Batch nextBatch = (Batch) in.readObject();
			
				for (Tuple nextTuple: nextBatch.getTuples()) {
					System.out.println("Tuple Nr. " + tupleNumber++);
					Object leftdata = nextTuple.dataAt(attrIndex);
	        		if(leftdata instanceof Integer){
	        		    System.out.println((Integer) leftdata);
	        		}else if(leftdata instanceof String){
	        			System.out.println((String) leftdata);
	
	        		}else if(leftdata instanceof Float){
	        			System.out.println((Float) leftdata);
	        		}
				}
			} catch (EOFException eof){
				break;
			}
		}
		in.close();
		
		System.out.println("=================================================");
		
		return tupleNumber;
    }
    
    private String temporaryFileName() {
    	filenum++;
        String filename = "MSJtemp-" + String.valueOf(filenum);
    	return filename;
    }
   
    private void getJoinAttrIndex() {
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
    }
    
    private void writeBlockToFile(ObjectOutputStream out, Batch nextBlock, int batchsize) throws IOException {
    	outBatch = new Batch(batchsize);
    	
    	while (!nextBlock.isEmpty()) {
    		if (outBatch.isFull()) {
    			out.writeObject(outBatch);
    			outBatch = new Batch(batchsize);
    		}
    		outBatch.add(nextBlock.head());
    		nextBlock.removeHead();
    	}
		
		if(!outBatch.isEmpty()) {
			out.writeObject(outBatch);
		}
    }
    
    private void nextSortedRun(Batch nextBlock, int attrIndex, int batchsize) throws IOException {
    	Collections.sort(nextBlock.getTuples(), (t1,t2) -> Tuple.compareTuples(t1, t2, attrIndex));
    	String tmpfname = temporaryFileName();
    	ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
		writeBlockToFile(tmpw, nextBlock, batchsize);
		tmpw.close();
		runfnames.add(tmpfname);
    }
    
    private boolean leftSortedRuns()  {
    	if (!left.open()) {
            return false;
        }
    	
    	Batch nextBlock = new Batch(numBuff*lbatchsize);
    	runfnames = new LinkedList<>();
    	
    	try {
	    	Batch nextBatch;
	        while ((nextBatch = left.next()) != null) {
	        	if (nextBatch.size() > 0) { // why necessary? bug
	                if(nextBlock.isFull()) {
	                	nextSortedRun(nextBlock, leftindex, lbatchsize);
	            		nextBlock = new Batch(numBuff*lbatchsize);
	                }
	                
	                for(Tuple nextTuple: nextBatch.getTuples()) { // All pages of size batchsize
	                	nextBlock.add(nextTuple);
	                }
	                lnumPages += 1;
	        	}
	        }
	        
	        nextSortedRun(nextBlock, leftindex, lbatchsize); // condition?
    	} catch (IOException io) {
            System.out.println("SortMerge: temporary file reading error");
            return false;
    	}
        
        if (!left.close()) {
        	return false;
        }
        
        return true;
    }
    
    private boolean rightSortedRuns() {
    	if (!right.open()) {
            return false; 
        }
    	
    	Batch nextBlock = new Batch(numBuff*rbatchsize);
    	runfnames = new LinkedList<>();

    	try {
	    	Batch nextBatch;
	        while ((nextBatch = right.next()) != null) {
	        	if (nextBatch.size() > 0) { // why necessary? bug
	                if(nextBlock.isFull()) {
	                	nextSortedRun(nextBlock, rightindex, rbatchsize);
	            		nextBlock = new Batch(numBuff*rbatchsize);
	            		numToRead = 0;
	                }
	                
	                for(Tuple nextTuple: nextBatch.getTuples()) {
	                	nextBlock.add(nextTuple);
	                }
	                rnumPages += 1;
	        	}
	        }
	        
	        nextSortedRun(nextBlock, rightindex, rbatchsize); // condition?
    	} catch (IOException io) {
            System.out.println("SortMerge: temporary file reading error");
            io.printStackTrace();
            return false;
    	}
        
        if (!right.close()) {
        	return false;
        }
        
        return true;
    }
    
    private int minTupleIndex(Batch[] inBatches, int attrIndex) {
    	Tuple minTuple = null;
    	int minTupleIndex = -1;
    	
    	for (int batchIndex = 0; batchIndex < numToRead; batchIndex++) { 
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
    
    private void writeRunsToMemory(Vector<ObjectInputStream> runFiles, Batch[] inBatches) 
    		throws IOException, ClassNotFoundException {

    	for (int runIndex = 0; runIndex < numToRead; runIndex++) {
			ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(runfnames.remove()));
			inBatches[runIndex] = ((Batch) nextStream.readObject()); // should be fine
			runFiles.add(nextStream);
			
			//int numBatches = (lastRun && (runIndex == (numToRead-1)))? finalNumBatches : runSize;
			//batchesPerRun[runIndex] = numBatches-1; // explain -1
			//numBatchesToMerge += numBatches;
		}
    }
    
    private void mergeRuns(ObjectOutputStream out, Batch[] inBatches, int runSize, int batchsize, int attrIndex) 
    		throws IOException, ClassNotFoundException {
    	
    	Vector<ObjectInputStream> runFiles = new Vector<>(numToRead);
		writeRunsToMemory(runFiles, inBatches);
		outBatch = new Batch(batchsize);
		
    	for (int batchIndex = 0; batchIndex < (runSize*(numBuff-1)); batchIndex++) {
			while(!outBatch.isFull()) {
				int minIndex = minTupleIndex(inBatches,attrIndex);
				if (minIndex == -1) {
					break;
				}
				
				Batch minBatch = inBatches[minIndex];
				Tuple minTuple = minBatch.head();
				outBatch.add(minTuple);
				minBatch.removeHead();
				
				if(minBatch.isEmpty()) {
					try {
						inBatches[minIndex] = (Batch) runFiles.get(minIndex).readObject();
					} catch (EOFException eof) {
						// Nothing should happen here
					}
				}
			}
			
			if(!outBatch.isEmpty()) {
				out.writeObject(outBatch);
				outBatch = new Batch(batchsize);
			} else {
				break;
			}
		}
    	
    	for (ObjectInputStream nextStream: runFiles) {
    		nextStream.close();
    	}
    }
    
    private void mergePhase(int numBatches, int batchsize, int attrIndex) throws IOException, ClassNotFoundException {
    	outBatch = new Batch(batchsize); 
    	Batch[] inBatches = new Batch[numBuff-1]; // define a constant 
    	String tmpMergedName = temporaryFileName(); 
		
    	int runSize = numBuff; // number of batches in a sorted run
		while (runSize < numBatches) { 
			
			int leftToMerge = runfnames.size();
			while (leftToMerge > 1) {
				
				numToRead = (leftToMerge <= (numBuff-1))? leftToMerge : (numBuff-1);
				
				ObjectOutputStream tmpMerged = new ObjectOutputStream(new FileOutputStream(tmpMergedName));
				mergeRuns(tmpMerged, inBatches, runSize, batchsize, attrIndex);
				tmpMerged.close();
				
				ObjectInputStream in = new ObjectInputStream(new FileInputStream(tmpMergedName));
				String mergefname = temporaryFileName();
				runfnames.add(mergefname);
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(mergefname));
				for (int batchIndex = 0; batchIndex < (runSize*(numBuff-1)); batchIndex++) {
					try {
						Batch nextBatch = (Batch) in.readObject();
						out.writeObject(nextBatch);
					} catch (EOFException eof) {
						break;
					}
				}
				in.close();
				out.close();
				
				leftToMerge -= numToRead;
			}
			runSize = runSize*(numBuff-1);
		} 
    }
    
    private boolean mergeSort(int numBatches, int batchsize, int attrIndex) {
    	try {
    	    mergePhase(numBatches, batchsize, attrIndex);
    		return true;
    	} catch (IOException io) {
    		io.printStackTrace();
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
    	if ((batchcurs == (numToRead-1)) && (rcurs == batchSizes[numToRead-1])) { // not entirely correct, change later
            close();
            return null;
        }
        
        outBatch = new Batch(jbatchsize); // This one is OK

        try {
        	if(numReadLeft == 0 && numReadRight == 0) {
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
	        	
	        	if (rcurs < (batchSizes[batchcurs]-1)) {
            		rcurs += 1;
            	} else if (batchcurs < (numToRead-1)) {
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
        	io.printStackTrace();
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
    	try {
			sortedLeft.close();
			sortedRight.close();
			for (int fnum = 1; fnum <= filenum; fnum++) { 
	    		String filename = "MSJtemp-" + String.valueOf(fnum);
	    		File f = new File(filename);
	    	    f.delete();
	    	}
		    return true;
		} catch (IOException e) {
			System.out.println("File closing error");
			return false;
		}
    }
    
}