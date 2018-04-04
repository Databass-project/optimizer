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
	
	private final Operator base;
	private final int numBuff;
	private final int batchSize;
	private final Comparator<Tuple> cmp;
	
	private int numPages;
	private String sortedName;
	
	private static int filenum = 0;
	private LinkedList<String> runfNames;
	private LinkedList<String> filesCreated;
	
	/* PUBLIC INTERFACE */
	
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
	
	public boolean sortedFile() {
		if (sortedRuns()) {
			boolean sorted = mergeSort();
			if(sorted) {
				sortedName = runfNames.removeFirst();
				filesCreated.removeLast(); // explain
				close();
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	public int getNumPages() {
		return numPages;
	}
	
	public String getSortedName() {
		return sortedName;
	}
	
	public void showFileContent(String fname, int numPages, int attrIndex) { // helper method
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
    		
    	}
		System.out.println("Number of tuples: " + tupleNumber);
		System.out.println("=================================================");
    }
	
	/* PRIVATE METHODS */
	
	private String temporaryFileName() {
    	filenum++;
        String filename = "SorterTemp-" + String.valueOf(filenum);
        filesCreated.add(filename);
    	return filename;
    }
	
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
    
    private void nextSortedRun(Batch nextBlock) throws IOException {
    	Collections.sort(nextBlock.getTuples(),cmp); 
    	String tmpfname = temporaryFileName();
    	ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
		writeBlockToFile(tmpw, nextBlock);
		tmpw.close();
		runfNames.add(tmpfname);
    }
    
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
	        	if (nextBatch.size() > 0) { // why necessary? bug
	                if(nextBlock.isFull()) {
	                	nextSortedRun(nextBlock);
	            		nextBlock = new Batch(numBuff*batchSize);
	                }
	                
	                for(Tuple nextTuple: nextBatch.getTuples()) { // All pages of size batchsize
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
    
    private void writeRunsToMemory(ObjectInputStream[] runFiles, Batch[] inBatches, int numToRead) 
    		throws IOException, ClassNotFoundException {

    	for (int runIndex = 0; runIndex < numToRead; runIndex++) {
			@SuppressWarnings("resource") // nextStream closed in mergeRuns
			ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(runfNames.remove())); 
			inBatches[runIndex] = ((Batch) nextStream.readObject()); // should be fine
			runFiles[runIndex] = nextStream;
		}
    }
    
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
    
    private void mergePhase() throws IOException, ClassNotFoundException {
    	Batch[] inBatches = new Batch[numBuff-1]; // define a constant 
		
    	int runSize = numBuff; // number of batches in a sorted run
		while (runSize < numPages) { 
			
			int leftToMerge = runfNames.size();
			while (leftToMerge > 1) {
				
				int numToRead = (leftToMerge <= (numBuff-1))? leftToMerge : (numBuff-1);
				
				String mergedName = temporaryFileName(); 
				ObjectOutputStream mergedRuns = new ObjectOutputStream(new FileOutputStream(mergedName));
				mergeRuns(mergedRuns, inBatches, runSize, numToRead);
				mergedRuns.close();
				runfNames.add(mergedName); // explain
				
				leftToMerge -= numToRead;
			}
			runSize = runSize*(numBuff-1);
		} 
    }
    
    private boolean mergeSort() {
    	try {
    	    mergePhase();
    		return true;
    	} catch (IOException io) {
    		io.printStackTrace();
            System.out.println("Sorter: temporary file RW error");
            return false;
        } catch (ClassNotFoundException c) {
            System.out.println("Sorter: deserialization error");
            return false;
        } 
    }
    
    private void close() { // don't delete sorted file
		for (String fname: filesCreated) { 
    		File f = new File(fname);
    	    f.delete();
    	}
		filesCreated = new LinkedList<>();
	}
}
