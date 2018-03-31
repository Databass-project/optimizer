/* BLOCK-nested join algorithm */

package qp.operators;

import qp.utils.*;


import java.io.*;
import java.util.*;
import java.lang.*;

public class BlockNestedJoin extends Join {
    int batchsize;  //Number of tuples per out batch

    /* The following fields are useful during execution of the NestedJoin operation */
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum = 0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftBlock;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)

    public BlockNestedJoin(Join jn) {
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

        if (!materializeTable()) return false;

        return (left.open());
    }

    /**
     * @return true if table is successfully written into a file. false otherwise
     */
    private boolean materializeTable() {
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
            System.out.println("NestedJoin: writing the temporary file error");
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
     * from input buffers select the tuples satisfying join condition. And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {

            if (lcurs == 0 && eosr) {
                leftBlock = fetchNextBlock();
                if (leftBlock == null) {
                    eosl = true;
                    return outbatch;
                }
                openRightTableFile();
            }

            while (!eosr) {

                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for (i = lcurs; i < leftBlock.size(); i++) {
                        for (j = rcurs; j < rightbatch.size(); j++) {
                            Tuple lefttuple = leftBlock.elementAt(i);
                            Tuple righttuple = rightbatch.elementAt(j);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);

                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    if (i == leftBlock.size() - 1 && j == rightbatch.size() - 1) {//case 1
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftBlock.size() - 1 && j == rightbatch.size() - 1) {//case 2
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else if (i == leftBlock.size() - 1 && j != rightbatch.size() - 1) {//case 3
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Some error in deserialization");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    private void openRightTableFile() {
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            eosr = false;
        } catch (IOException io) {
            System.err.println("BlockNestedJoin: error in reading the file");
            System.exit(1);
        }
    }

    /**
     * creates leftBatch whose size is at most numBuffers-2.
     * @return batch representing next block. Null if no batch was read.
     */
    public Batch fetchNextBlock() {
        ArrayList<Batch> nextBatches = new ArrayList<>();
        for (int i = 0; i < numBuff-2; i++) {
            Batch next = left.next();
            if (next == null) 
                break;
            nextBatches.add(next);
        }
        if (nextBatches.size() == 0) // no batches were added
            return null;
        Batch nextBlock = new Batch(batchsize*nextBatches.size());
        for(Batch b: nextBatches) {
            for (int j = 0; j < b.size(); j++) {
                nextBlock.add(b.elementAt(j));
            }
        }
        return nextBlock;
    }

    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}


//package qp.operators;
//
//import qp.utils.Attribute;
//import qp.utils.Batch;
//import qp.utils.Schema;
//import qp.utils.Tuple;
//
//import java.io.*;
//import java.util.ArrayList;
//
//public class BlockNestedJoin extends Join {
//    private int leftJoinAttrIndex;
//    private int rightJoinAttrIndex;
//    private int rightCursor;
//    public int joinType;
//    private Batch outBuffer;
//    private Block block;
//    private Batch rightBuffer;
//    private Schema schema;
//    private int numTuplesPerPage; // for the output buffer
//
//    private boolean isEOSRight;
//
//    // table materialization
//    private static int tempFileNum = 0;
//    private static String tempFileName;
//    private ObjectInputStream in;
//
//    public BlockNestedJoin(Join join, int numBuffers) {
//        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
//        if (numBuffers < 3) {
//            throw new IllegalArgumentException("Number of buffers < 3. Not enough to carry out join");
//        }
//        schema = join.getSchema();
//        joinType = JoinType.NESTEDJOIN;
//        block = new Block(numBuffers-2, join.getLeft());
//    }
//
//    public boolean open() {
//        rightCursor = 0;
//        isEOSRight = true;
//        findJoinIndexes();
//        numTuplesPerPage = Batch.getPageSize() / schema.getTupleSize();
//
//        if (!right.open())
//            return false;
//
//        if (!materializeTable()) return false;
//
//        return (right.close() && left.open());
//    }
//
//    public Batch next() {
//        if (isEndOfJoin()) {
//            close();
//            return null;
//        }
//        outBuffer = new Batch(numTuplesPerPage);
//
//        while(!outBuffer.isFull()) {
//            // new block to fetch
//            if (isEOSRight &&  ) {
//                block.loadNextBlock();
//            }
//
//            if (isEndOfJoin()) {
//                return outBuffer;
//            }
//
//
//
//        }
//
//        return null;
//    }
//
//    private void
//    private boolean isEndOfJoin() {
//        return (block.isLastBlock() && isEOSRight);
//    }
//
//    public boolean close() {
//        block = null;
//        // simply discard the file
//        File f = new File(tempFileName);
//        return f.delete();
//    }
//
//    private void findJoinIndexes() {
//        leftJoinAttrIndex = left.getSchema().indexOf(con.getLhs());
//        rightJoinAttrIndex = right.getSchema().indexOf((Attribute)con.getRhs());
//    }
//
//    /**
//     * writes the table into a temporary file so that it can be scanned repeatedly
//     * @return true if the table is written to file without error
//     */
//    private boolean materializeTable() {
//        tempFileName = "BNLJ-temp" + tempFileNum;
//        try {
//            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFileName));
//            Batch tempBatch;
//            while( (tempBatch = right.next() ) != null) {
//                out.writeObject(tempBatch);
//            }
//        } catch (IOException ioe) {
//            System.out.println("BNLJ: cannot materialize right table");
//            return false;
//        }
//        return true;
//    }
//
//}
//
//class Block {
//    private ArrayList<Batch> batches;
//    private BNLCursor cursor;
//    private Operator left;
//    private int numTuplesPerBatch;
//    private boolean isLastBlock;
//
//    public Block(int numBuffers, Operator left) {
//        batches = new ArrayList<>(numBuffers);
//        this.left = left;
//        numTuplesPerBatch = Batch.getPageSize() / left.getSchema().getTupleSize();
//        cursor = new BNLCursor(0, 0);
//        isLastBlock = false;
//    }
//
//    /**
//     * @return next tuple
//     */
//    private Tuple getNextTuple() {
//        Tuple tupleToReturn = batches.get(cursor.blockCursor).elementAt(cursor.tupleCursor);
//
//        // update blockIndex and tupleIndex
//        cursor.tupleCursor = (cursor.tupleCursor + 1) % numTuplesPerBatch;
//        if (cursor.tupleCursor == 0)
//            // then go to next block
//            cursor.blockCursor += (cursor.blockCursor+1) % batches.size(); // go ba
//        return tupleToReturn;
//    }
//
//    /**
//     * loads next block from left table. Call this function once one scan of right table is done.
//     */
//    public void loadNextBlock() {
//        if (isLastBlock) {
//            System.out.println("No more block to read from the left block");
//            return;
//        }
//
//        cursor.blockCursor = 0;
//        cursor.tupleCursor = 0;
//        for (int i = 0; i < batches.size(); i++) {
//            Batch nextBatch = left.next();
//            if (nextBatch == null) {
//                isLastBlock = true;
//                return;
//            }
//            batches.set(i, nextBatch);
//        }
//    }
//
//    public boolean isLastBlock() {
//        return isLastBlock;
//    }
//
//    class BNLCursor {
//        public int blockCursor;
//        public int tupleCursor;
//
//        public BNLCursor(int block, int tuple) {
//            blockCursor = block;
//            tupleCursor = tuple;
//        }
//    }
//
//
//}
