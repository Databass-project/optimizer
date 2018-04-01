/** To projec out the required attributes from the result **/

package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
public class OrderBy extends Operator{

    Operator base;
    Vector attrSet;
	int batchsize;  // number of tuples per outbatch
    static int filenum = 0;   // To get unique filenum for this operation
    String fname;    // The file name where the base table is materialized
    LinkedList<String> runfnames;
    int numPages; // Number of memory pages of base input stream
    int numBuff; 
    ObjectInputStream in;      // Base file being scanned
    /** The following fields are requied during execution
     ** of the Project Operator
     **/

    Batch inbatch;
    Batch outBatch;
    
    int finalNumBatches = 0;
    int numToRead = 0;
    
    boolean eosb = false;

    /** index of the attributes in the base operator
     ** that are to be projected
     **/

    int[] attrIndices;


    public OrderBy(Operator base, Vector as, int type){
	super(type);
	this.base=base;
	this.attrSet=as;

    }

    /* number of buffers available to this OrderBy operator */

    public void setNumBuff(int num) {
        this.numBuff = num;
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
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open(){
		/** setnumber of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;
	
		Schema baseSchema = base.getSchema();
		attrIndices = new int[attrSet.size()];
		//System.out.println("OrderBy---Schema: ----------in open-----------");
		//System.out.println("base Schema---------------");
		//Debug.PPrint(baseSchema);
		for(int i=0;i<attrSet.size();i++){
		    Attribute attr = (Attribute) attrSet.elementAt(i);
	  	    int index = baseSchema.indexOf(attr);
		    attrIndices[i]=index;
	
		    //  Debug.PPrint(attr);
		    //System.out.println("  "+index+"  ");
		}
	
		try {
        	if (leftSortedRuns()) {
	        	/**int numTuples = 0;
	        	for (String fname: runfnames) {
	        		numTuples += showFileContent(fname, numBuff, leftindex);
	        	}
	        	System.out.println("Total number of tuples: " + numTuples);**/
	        	if (mergeSort(fname, numPages, batchsize)) {
	        		fname = runfnames.removeFirst();
	        		/**int numTuples = showFileContent(lfname, lnumPages, leftindex);
	        		System.out.println("Left number of tuples: " + numTuples);**/
		            in = new ObjectInputStream(new FileInputStream(fname));
		            return true;
	        	} else {
	        		return false; // print
	        	}
        } else {
        		return false; // print
        	}
        } catch (IOException io) {
            System.out.println("SortMerge: temporary file reading error");
            return false;
        }
    }
    
    //TODO USE CODE FOR MERGESORT IN UTILS OR WHEREVER
    public boolean mergesort(String bname, int bnumPages, Vector attrSet){
    	return true;
    }


    /** Read next tuple from operator */

    public Batch next(){
    //System.out.println("OrderBy---Schema: ----------in next-----------");
    if (eosb) {
        close();
        return null;
    }	
    	
	outBatch = new Batch(batchsize);

	try {
		outBatch = (Batch) in.readObject();
	}catch (EOFException e) {
        try {
            in.close();
        } catch (IOException io) {
            System.out.println("OrderBy: Error in temporary file reading");
        }
        eosb = true;
    } catch (ClassNotFoundException c) {
        System.out.println("OrderBy: Some error in deserialization ");
        System.exit(1);
    } catch (IOException io) {
        System.out.println("OrderBy: temporary file reading error");
        System.exit(1);
    }
	
		return outBatch;
    }

    
    private String temporaryFileName() {
    	filenum++;
        String filename = "OBtemp-" + String.valueOf(filenum);
    	return filename;
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
    
    private void nextSortedRun(Batch nextBlock, int batchsize) throws IOException {
    	Collections.sort(nextBlock.getTuples(), (t1,t2) -> Tuple.compareTuplesWith(t1, t2, attrIndices));
    	String tmpfname = temporaryFileName();
    	ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
		writeBlockToFile(tmpw, nextBlock, batchsize);
		tmpw.close();
		runfnames.add(tmpfname);
    }
    
    private boolean leftSortedRuns()  {
    	if (!base.open()) {
            return false;
        }
    	
    	Batch nextBlock = new Batch(numBuff*batchsize);
    	runfnames = new LinkedList<>();
    	
    	try {
	    	Batch nextBatch;
	    	numToRead = 0;
	        while ((nextBatch = base.next()) != null) {
	        	if (nextBatch.size() > 0) { // why necessary? bug
	                if(nextBlock.isFull()) {
	                	nextSortedRun(nextBlock, batchsize);
	            		nextBlock = new Batch(numBuff*batchsize);
	            		numToRead = 0;
	                }
	                
	                for(Tuple nextTuple: nextBatch.getTuples()) { // All pages of size batchsize
	                	nextBlock.add(nextTuple);
	                }
	                numPages += 1;
	                numToRead += 1;
	        	}
	        }
	        
	        nextSortedRun(nextBlock, batchsize); // condition?
	        finalNumBatches = numToRead; // explain
    	} catch (IOException io) {
            System.out.println("SortMerge: temporary file reading error");
            return false;
    	}
        
        if (!base.close()) {
        	return false;
        }
        
        return true;
    }
    
    private int minTupleIndex(Batch[] inBatches) {
    	Tuple minTuple = null;
    	int minTupleIndex = -1;
    	
    	for (int batchIndex = 0; batchIndex < numToRead; batchIndex++) { 
    		Batch nextBatch = inBatches[batchIndex];
    		
    		if(!nextBatch.isEmpty()) {
    			Tuple nextTuple = nextBatch.head();
    			
    			if((minTupleIndex == -1) || (Tuple.compareTuplesWith(nextTuple, minTuple, attrIndices) < 0)) {
    				minTuple = nextTuple;
    				minTupleIndex = batchIndex;
    			} 
    		}
    	}
    	
    	return minTupleIndex;
    }
    
    private int writeRunsToMemory(Vector<ObjectInputStream> runFiles, int[] batchesPerRun, Batch[] inBatches, int runSize, boolean lastRun) 
    		throws IOException, ClassNotFoundException {
    	
    	int numBatchesToMerge = 0;
    	
    	String fname = ""; // explain
    	for (int runIndex = 0; runIndex < numToRead; runIndex++) {
    		fname = runfnames.remove();
			ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(fname));
			inBatches[runIndex] = ((Batch) nextStream.readObject()); // should be fine
			runFiles.add(nextStream);
			
			int numBatches = (lastRun && (runIndex == (numToRead-1)))? finalNumBatches : runSize;
			batchesPerRun[runIndex] = numBatches-1; // explain -1
			numBatchesToMerge += numBatches;
		}
   
    	runfnames.add(fname); // explain
    	
    	return numBatchesToMerge;
    }
    
    private int mergeRuns(ObjectOutputStream out, Batch[] inBatches, int runSize, int batchsize, boolean lastRun) 
    		throws IOException, ClassNotFoundException {
    	
    	Vector<ObjectInputStream> runFiles = new Vector<>(numToRead);
		int[] batchesPerRun = new int[numToRead];
		int numBatchesToMerge = writeRunsToMemory(runFiles, batchesPerRun, inBatches, runSize, lastRun);
		outBatch = new Batch(batchsize);
		
    	for (int mergedIndex = 0; mergedIndex < numBatchesToMerge; mergedIndex++) {
			for (int tupleIndex = 0; tupleIndex < batchsize; tupleIndex++) {
				int minIndex = minTupleIndex(inBatches);
				if (minIndex == -1) {
					break;
				}
				
				Batch minBatch = inBatches[minIndex];
				Tuple minTuple = minBatch.head();
				outBatch.add(minTuple);
				minBatch.removeHead();
				
				if(minBatch.isEmpty()) {
					if (batchesPerRun[minIndex] > 0) {
						inBatches[minIndex] = (Batch) runFiles.get(minIndex).readObject();
						batchesPerRun[minIndex] -= 1;
					}
				}
			}
			
			out.writeObject(outBatch);
			outBatch = new Batch(batchsize);
		}
    	
    	for (ObjectInputStream nextStream: runFiles) {
    		nextStream.close();
    	}
    	
    	return numBatchesToMerge;
    }
    
    private void mergePhase(int numBatches, int batchsize) throws IOException, ClassNotFoundException {
    	outBatch = new Batch(batchsize); 
    	Batch[] inBatches = new Batch[numBuff-1]; // define a constant 
    	String tmpMergedName = temporaryFileName(); 
		
    	int runSize = numBuff; // number of batches in a sorted run
		while (runSize < numBatches) { 
			
			int leftToMerge = runfnames.size();
			while (leftToMerge > 1) {
				
				numToRead = numBuff-1;
				boolean lastRun = false;
				if (leftToMerge <= (numBuff-1)) {
					numToRead = leftToMerge;
					lastRun = true;
				}
				
				ObjectOutputStream tmpMerged = new ObjectOutputStream(new FileOutputStream(tmpMergedName));
				int numBatchesMerged = mergeRuns(tmpMerged, inBatches, runSize, batchsize, lastRun);
				tmpMerged.close();
				
				ObjectInputStream in = new ObjectInputStream(new FileInputStream(tmpMergedName));
				String mergefname = runfnames.getLast(); // Merged-file name was appended
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(mergefname));
				for (int batchIndex = 0; batchIndex < numBatchesMerged; batchIndex++) {
					out.writeObject(in.readObject());
				}
				in.close();
				out.close();
				
				if(lastRun) {
					finalNumBatches += (numToRead-1)*runSize; // explain
				}
				leftToMerge -= numToRead;
				
			}
			runSize = runSize*(numBuff-1);
		} 
    }
    
    private boolean mergeSort(String filename, int numBatches, int batchsize) {
    	try {
    	    mergePhase(numBatches, batchsize);
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

 // Delete temporary files
    public boolean close() {
    	try {
			in.close();
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
