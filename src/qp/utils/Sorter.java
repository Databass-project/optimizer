package qp.utils;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import qp.operators.Operator;

public final class Sorter {
	
	private final Operator base;				// The operator that generates the table to sort
	private final int numBuff;					// The number of buffers that can be used 
	private final int batchSize;				// The number of buffers that can be used
	private final Comparator<Tuple> cmp;		// Defines how tuples are compared
	
	private int numPages;						// The number of pages of the materialized table
	private String sortedName;					// The file-name of the materialized, sorted file
	
	private static int filenum = 0;				// To get unique filenum for this operation
	private LinkedList<String> runfNames;		// Sorted runs file-names
	private LinkedList<String> filesCreated;	// Names of all the files created during sorting process
	
	/* =============================== PUBLIC INTERFACE =============================== */ 
	
	/**
	 * Creates a new Sorter, which will sort and materialize the table produced by base
	 * @param base, the operator that generates the table to sort
	 * @param numBuff, the number of buffers that can be used 
	 * @param batchSize, the number of tuples per buffer
	 * @param cmp, which defines how tuples are compared
	 */
	public Sorter(Operator base, int numBuff, int batchSize, Comparator<Tuple> cmp) {
		this.base = base;
		this.numBuff = numBuff;
		this.batchSize = batchSize;
		this.cmp = cmp;
		numPages = 0;
		sortedName = "Not yet sorted";
		runfNames = new LinkedList<>();
		filesCreated = new LinkedList<>();
	}
	
	/**
	 * Sorts and materializes the input table, saves the sorted file name and number of pages
     * @return true if the table was properly sorted and materialized
     */
	public boolean sortedFile() {
		if (sortedRuns()) {
			boolean sorted = mergePhase();
			if(sorted) {
				sortedName = runfNames.removeFirst();
				filesCreated.removeLast(); // sorted file should not be deleted in close()
				close();
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	/**
     * @return the number of pages of the materialized table
     */
	public int getNumPages() {
		return numPages;
	}
	
	/**
     * @return the file-name of the materialized, sorted file
     */
	public String getSortedName() {
		return sortedName;
	}
	
	/**
     * Helper method that prints a file's content
     */
	public void showFileContent(String fname, int numPages, int attrIndex) { 
    	int tupleNumber = 0;
    	
    	try {
    		
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(fname));
			
			for (int batchIndex = 0; batchIndex < numPages; batchIndex++) {
					Batch nextBatch = (Batch) in.readObject();
				
					for (Tuple nextTuple: nextBatch.getTuples()) {
						System.out.println("Tuple Nr. " + tupleNumber++);
						Object leftdata = nextTuple.dataAt(attrIndex);
		        		if (leftdata instanceof Integer){
		        		    System.out.println((Integer) leftdata);
		        		} else if(leftdata instanceof String){
		        			System.out.println((String) leftdata);
		
		        		} else if(leftdata instanceof Float){
		        			System.out.println((Float) leftdata);
		        		}
					}
				
			}
			in.close();
			
    	} catch (Exception e) {
    		// nothing should be done here
    	}
		System.out.println("Number of tuples: " + tupleNumber);
		System.out.println("=================================================");
    }
	
	/* =============================== PRIVATE METHODS =============================== */ 

	/**
	 * @return a new temporary-file-name
	 */
	private String temporaryFileName() {
    	filenum++;
        String filename = "SorterTemp-" + String.valueOf(filenum);
        filesCreated.add(filename);
    	return filename;
    }
	
	/**
	 * Writes a block of pages to a file
	 * @param out, the file
	 * @param nextBlock, numBuff memory pages
	 */
	private void writeBlockToFile(ObjectOutputStream out, Batch nextBlock) throws IOException {
    	Batch outBatch = new Batch(batchSize);
    	
    	while (!nextBlock.isEmpty()) {
    		if (outBatch.isFull()) {
    			out.writeObject(outBatch);
    			outBatch = new Batch(batchSize);
    		}
    		outBatch.add(nextBlock.head());
    		nextBlock.removeHead();
    	}
		
		if(!outBatch.isEmpty()) {
			out.writeObject(outBatch);
		}
    }
   
	/**
	 * Creates the next sorted run and saves it in a temporary file
	 * @param nextBlock
	 */
    private void nextSortedRun(Batch nextBlock) throws IOException {
    	Collections.sort(nextBlock.getTuples(),cmp); 
    	String tmpfname = temporaryFileName();
    	ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
		writeBlockToFile(tmpw, nextBlock);
		tmpw.close();
		runfNames.add(tmpfname);
    }
    
    /**
     * Generates all the sorted runs
     * @return true if the sorted runs were correctly created and materialized
     */
    private boolean sortedRuns()  {
    	if (!base.open()) {
            return false;
        }
    	
    	runfNames = new LinkedList<>();
    	numPages = 0;
    	Batch nextBlock = new Batch(numBuff*batchSize);
    	
    	try {
	    	Batch nextBatch;
	        while ((nextBatch = base.next()) != null) {
	        	if (nextBatch.size() > 0) { 
	                if(nextBlock.isFull()) {
	                	nextSortedRun(nextBlock);
	            		nextBlock = new Batch(numBuff*batchSize);
	                }
	                
	                for(Tuple nextTuple: nextBatch.getTuples()) { 
	                	nextBlock.add(nextTuple);
	                }
	                numPages += 1;
	        	}
	        }
	        
	        nextSortedRun(nextBlock); 
    	} catch (IOException io) {
            System.out.println("Sorter: temporary file RW error");
            return false;
    	}
        
        if (!base.close()) {
        	return false;
        }
        
        return true;
    }
    
    /**
     * Finds the buffer-index in memory of the buffer that contains the smallest tuple
     * @param inBatches, (numBuff-1) memory buffers, where we save the pages of runs to be merged
     * @param numToRead, the number of valid memory buffers (valid: not from previous iteration)
     * @return -1 if all the tuples were read, else the buffer-index
     */
    private int minTupleIndex(Batch[] inBatches, int numToRead) {
    	Tuple minTuple = null;
    	int minTupleIndex = -1;
    	
    	for (int batchIndex = 0; batchIndex < numToRead; batchIndex++) { 
    		Batch nextBatch = inBatches[batchIndex];
    		
    		if(!nextBatch.isEmpty()) {
    			Tuple nextTuple = nextBatch.head();
    			
    			if((minTupleIndex == -1) || (cmp.compare(nextTuple, minTuple) < 0)) {
    				minTuple = nextTuple;
    				minTupleIndex = batchIndex;
    			} 
    		}
    	}
    	
    	return minTupleIndex;
    }
    
    /**
     * Loads the numToRead sorted runs into memory, and reads their first pages
     * @param runFiles, array of file-pointers of the files that contain the runs to be merged
     * @param inBatches, (numBuff-1) memory buffers
     * @param numToRead, the number of runs to merge
     */
    private void writeRunsToMemory(ObjectInputStream[] runFiles, Batch[] inBatches, int numToRead) 
    		throws IOException, ClassNotFoundException {

    	for (int runIndex = 0; runIndex < numToRead; runIndex++) {
			ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(runfNames.remove())); // closed later
			inBatches[runIndex] = ((Batch) nextStream.readObject()); 
			runFiles[runIndex] = nextStream;
		}
    }
    
    /**
     * Merge numToRead runs
     * @param out, the temporary file where the merged runs are saved
     * @param inBatches, (numBuff-1) memory buffers
     * @param runSize, the current size (in pages) of a sorted run
     * @param numToRead, the number of runs to merge
     */
    private void mergeRuns(ObjectOutputStream out, Batch[] inBatches, int runSize, int numToRead) 
    		throws IOException, ClassNotFoundException {
    	
    	ObjectInputStream[] runFiles = new ObjectInputStream[numToRead];
		writeRunsToMemory(runFiles, inBatches, numToRead);
		Batch outBatch = new Batch(batchSize);
		
    	for (int batchIndex = 0; batchIndex < (runSize*(numBuff-1)); batchIndex++) {
			while(!outBatch.isFull()) {
				int minIndex = minTupleIndex(inBatches, numToRead);
				if (minIndex == -1) {
					break;
				}
				
				Batch minBatch = inBatches[minIndex];
				Tuple minTuple = minBatch.head();
				outBatch.add(minTuple);
				minBatch.removeHead();
				
				if(minBatch.isEmpty()) {
					try {
						inBatches[minIndex] = (Batch) runFiles[minIndex].readObject();
					} catch (EOFException eof) {
						// Nothing should happen here
					}
				}
			}
			
			if(!outBatch.isEmpty()) {
				out.writeObject(outBatch);
				outBatch = new Batch(batchSize);
			} else {
				break;
			}
		}
    	
    	for (ObjectInputStream nextStream: runFiles) {
    		nextStream.close();
    	}
    }
    
    /**
     * merge-part of Multi-way Merge-Sort
     * @return true if all the sorted runs were merged
     */
    private boolean mergePhase()  {
    	try { 
	    	Batch[] inBatches = new Batch[numBuff-1]; 
			
	    	int runSize = numBuff; 
			while (runSize < numPages) { 
				
				int leftToMerge = runfNames.size();
				while (leftToMerge > 1) {
					
					int numToRead = (leftToMerge <= (numBuff-1))? leftToMerge : (numBuff-1);
					
					String mergedName = temporaryFileName(); 
					ObjectOutputStream mergedRuns = new ObjectOutputStream(new FileOutputStream(mergedName));
					mergeRuns(mergedRuns, inBatches, runSize, numToRead);
					mergedRuns.close();
					runfNames.add(mergedName); 
					
					leftToMerge -= numToRead;
				}
				runSize = runSize*(numBuff-1);
			}
			return true;
    	} catch (IOException io) {
            System.out.println("Sorter: temporary file RW error");
            return false;
        } catch (ClassNotFoundException c) {
            System.out.println("Sorter: deserialization error");
            return false;
        } 
    }
    
    /**
     * Deletes the files created during sorting process
     */
    private void close() { 
		for (String fname: filesCreated) { 
    		File f = new File(fname);
    	    f.delete();
    	}
		filesCreated = new LinkedList<>();
	}
}
