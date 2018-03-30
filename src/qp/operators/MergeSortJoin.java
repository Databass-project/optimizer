package qp.operators;

import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;

import com.sun.org.apache.xerces.internal.impl.dv.xs.SchemaDateTimeException;

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
    HashMap<String,Integer> batchesPerRun;

    static int filenum = 0;   // To get unique filenum for this operation

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
        
        boolean materialized = materializeLeftTable() && materializeRightTable();
        boolean sorted = mergeSort(lfname,lnumPages,leftindex) && mergeSort(rfname,rnumPages,rightindex);

        return materialized && sorted; 
    }
    
    private String temporaryFileName() {
    	filenum++;
        String filename = "NJtemp-" + String.valueOf(filenum);
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
        if (!right.close())
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
    	batchesPerRun = new HashMap<>();
    	ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
		
		int leftToRead = numBatches;
		while (leftToRead > 0) {
			int numRead = (leftToRead < numBuff)? leftToRead : numBuff;
			Batch nextBlock = fetchNextBlock(in,numRead);
			Collections.sort(nextBlock.getTuples(), (t1,t2) -> Tuple.compareTuples(t1, t2, attrIndex));
			
			// Creates a new file for a sorted run
			String tmpfname = temporaryFileName();
			ObjectOutputStream tmpw = new ObjectOutputStream(new FileOutputStream(tmpfname));
			writeBlockToFile(tmpw,nextBlock);
			tmpw.close();
			runfnames.add(tmpfname);
			batchesPerRun.put(tmpfname, numRead);
			
			leftToRead -= numRead;
		}
		
    	in.close();
    }
    
    private int minTupleIndex(ArrayList<Batch> inBatches, int attrIndex) {
    	Tuple minTuple = null;
    	int minTupleIndex = -1;
    	
    	for (int batchIndex = 0; batchIndex < inBatches.size(); batchIndex++) {
    		Batch nextBatch = inBatches.get(batchIndex);
    		
    		if(!nextBatch.isEmpty()) {
    			Tuple nextTuple = nextBatch.elementAt(0);
    			
    			if((minTupleIndex == -1) || (Tuple.compareTuples(nextTuple, minTuple, attrIndex) < 0)) {
    				minTuple = nextTuple;
    				minTupleIndex = batchIndex;
    			} 
    		}
    	}
    	
    	return minTupleIndex;
    }
    
    private void mergePhase(int attrIndex) throws IOException, ClassNotFoundException {
    	// Problem with having contiguous Buffer (instead of B-1) -> no direct access to sub-buffers 
    	Batch outBatch = new Batch(batchsize); // newly defined, because 1 & (B-1) buffers
    	ArrayList<Batch> inBatches = new ArrayList<>(numBuff-1); // define a constant 
    	String tmpMergedName = temporaryFileName(); 
		
		while (runfnames.size() > 1) { 
			
			int leftToMerge = runfnames.size();
			while (leftToMerge > 0) {
				
				int numMerge = (leftToMerge < (numBuff-1))? leftToMerge : (numBuff-1);
				int numBatchesToMerge = 0;
				ArrayList<ObjectInputStream> runFiles = new ArrayList<>(numMerge);
				ObjectOutputStream tmpMerged = new ObjectOutputStream(new FileOutputStream(tmpMergedName));
				ArrayList<String> fnames = new ArrayList<>(numMerge);
				
				String mergefname = runfnames.remove();
				fnames.add(mergefname);
				ObjectInputStream nextStream = new ObjectInputStream(new FileInputStream(mergefname));
				inBatches.add((Batch) nextStream.readObject());
				batchesPerRun.put(mergefname,batchesPerRun.get(mergefname)-1);
				runFiles.add(nextStream);
				
				numBatchesToMerge += batchesPerRun.get(mergefname);
				
				for(int runIndex = 1; runIndex < numMerge; runIndex++) {
					String fname = runfnames.remove();
					fnames.add(fname);
					nextStream = new ObjectInputStream(new FileInputStream(fname));
					inBatches.add((Batch) nextStream.readObject());
					batchesPerRun.put(fname,batchesPerRun.get(fname)-1);
					runFiles.add(nextStream);
					
					numBatchesToMerge += batchesPerRun.get(fname);
				}
				
				// At this point we have the opened streams of the runs we want to merge
				// Create a new temporary file, then copy its content to mergefname
				for (int mergedIndex = 0; mergedIndex < numBatchesToMerge; mergedIndex++) {
					for (int tupleIndex = 0; tupleIndex < batchsize; tupleIndex++) {
						int minIndex = minTupleIndex(inBatches,attrIndex);
						if (minIndex == -1) {
							break;
						}
						
						Tuple minTuple = inBatches.get(minIndex).elementAt(0);
						outBatch.add(minTuple);
						inBatches.get(minIndex).remove(0);
						
						if(inBatches.get(minIndex).isEmpty()) {
							String fname = fnames.get(minIndex);
							if (batchesPerRun.get(fname) > 0) {
								inBatches.set(minIndex,(Batch) runFiles.get(minIndex).readObject());
								batchesPerRun.put(fname,batchesPerRun.get(fname)-1);
							}
						}
					}
					
					tmpMerged.writeObject(outBatch);
					outBatch.clear();
				}
				
				for(int runIndex = 0; runIndex < numMerge; runIndex++) {
					runFiles.get(runIndex).close();
				}
				
				inBatches.clear();
				tmpMerged.close();
				
				ObjectInputStream in = new ObjectInputStream(new FileInputStream(tmpMergedName));
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(mergefname));
				for (int batchIndex = 0; batchIndex < numBatchesToMerge; batchIndex++) {
					out.writeObject(in.readObject());
				}
				in.close();
				out.close();
				 
				
				runfnames.add(mergefname);
				batchesPerRun.put(mergefname,numBatchesToMerge);
				leftToMerge -= numMerge;
			}
		} 
    }
    
    private boolean mergeSort(String filename, int numBatches, int attrIndex) {
    	try {
    		createSortedRuns(filename, numBatches, attrIndex);
    		mergePhase(attrIndex);
    		return true;
    	} catch (IOException io) {
            System.out.println("MergeSortJoin: file RW error");
            return false;
        } catch (ClassNotFoundException c) {
            System.out.println("MergeSortJoin: Some error in deserialization");
            return false;
        } 
    }
    
    
    public Batch next() {
    	return null;
    }
    
    
    // Delete temporary files, save first fnum
    public boolean close() {
    	return false;
    }
    
}
