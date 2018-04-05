/* This method calculates the cost of the generated plans, and also estimates the statistics of the result relation */
package qp.optimizer;

import qp.operators.*;
import qp.utils.*;

import java.util.Hashtable;
import java.util.StringTokenizer;
import java.io.*;

public class PlanCost {
    int cost;
    int numtuple;

    /**
     * If buffers are not enough for a selected join, then this plan is not feasible and return a cost of infinity.
     **/
    boolean isFeasible;

    /**
     * stores mapping from Attribute name to number of distinct values of that attribute
     **/
    Hashtable<Attribute, Integer> attrToV;

    public PlanCost() {
        attrToV = new Hashtable();
        cost = 0;
    }

    /**
     * @return the cost of the plan
     **/
    public int getCost(Operator root) {
        isFeasible = true;
        numtuple = calculateCost(root);
        return isFeasible? cost: Integer.MAX_VALUE;
    }

    /**
     * get number of tuples in estimated results
     **/
    public int getNumTuples() {
        return numtuple;
    }

    /**
     * returns number of tuples in the root
     **/
    protected int calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        }
        return -1;
    }

    /**
     * projection will not change any statistics. No cost involved as done on the fly
     **/
    protected int getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * calculates the statistics, and cost of join operation
     **/
    protected int getStatistics(Join node) {
        int lefttuples = calculateCost(node.getLeft()); // this part is recursive
        int righttuples = calculateCost(node.getRight());
        if (!isFeasible) {
            return -1; // shouldn't this return Integer.MAX_VALUE?
        }

        Condition con = node.getCondition();
        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /* get size of the tuple in output & correspondingly calculate buffer capacity, i.e., number of tuples per page **/
        int tuplesize = node.getSchema().getTupleSize();
        int outcapacity = Batch.getPageSize() / tuplesize;
        int leftTupleSizeInBytes = leftschema.getTupleSize();
        int numTuplesPerPageForLeft = Batch.getPageSize() / leftTupleSizeInBytes;
        int rightTupleSizeInBytes = rightschema.getTupleSize();
        int numTuplesPerPageForRight = Batch.getPageSize() / rightTupleSizeInBytes;

        if (Batch.getPageSize() < leftTupleSizeInBytes || Batch.getPageSize() < rightTupleSizeInBytes) {
            System.out.println("The buffer cannot hold entire tuple for right/left tables. Exiting now");
            System.exit(1);
        }

        int leftpages = (int) Math.ceil(((double) lefttuples) / (double) numTuplesPerPageForLeft);
        int rightpages = (int) Math.ceil(((double) righttuples) / (double) numTuplesPerPageForRight);

        Attribute leftjoinAttr = con.getLhs();
        Attribute rightjoinAttr = (Attribute) con.getRhs();
        int leftattrind = leftschema.indexOf(leftjoinAttr);
        int rightattrind = rightschema.indexOf(rightjoinAttr);
        leftjoinAttr = leftschema.getAttribute(leftattrind);
        rightjoinAttr = rightschema.getAttribute(rightattrind);
        /* number of distinct values of left and right join attribute */
        int leftattrdistn =  attrToV.get(leftjoinAttr);
        int rightattrdistn = attrToV.get(rightjoinAttr);

        int outtuples = (int) Math.ceil(((double) lefttuples * righttuples) / (double) Math.max(leftattrdistn, rightattrdistn));
        int minDistinct = Math.min(leftattrdistn, rightattrdistn);
        attrToV.put(leftjoinAttr, minDistinct);
        attrToV.put(leftjoinAttr, minDistinct);

        /* now calculate the cost of the operation */
        int joinType = node.getJoinType();
        int numbuff = BufferManager.getBuffersPerJoin();
        if (numbuff == 0) {
            System.out.println("#buffers is not set. Exiting code");
            System.exit(1);
        }
        int joincost;
        switch (joinType) {
            case JoinType.NESTEDJOIN:
                joincost = leftpages + (leftpages * rightpages);
                break;
            case JoinType.BLOCKNESTED:
                joincost = leftpages + (int) (Math.ceil((double) leftpages / (numbuff - 2))) * rightpages;
                break;
            case JoinType.SORTMERGE:
                joincost = 2 * leftpages * (1 + getCeilLog((int) Math.ceil((double)leftpages/ numbuff), numbuff-1));
                joincost += leftpages;
                joincost += rightpages;
                joincost += 2 * rightpages * (1 + getCeilLog((int) Math.ceil((double) rightpages/ numbuff), numbuff-1));
                break;
            case JoinType.HASHJOIN:
                joincost = 0;
                break;
            default:
                joincost = 0;
                break;
        }
        cost = cost + joincost;
        return outtuples;
    }

    /**
     * 1. Find # of incoming tuples using the selectivity.
     * 2. Find # of output tuples and statistics about the attributes
     * Selection is performed on the fly, so no cost involved
     **/
    protected int getStatistics(Select node) {
        int intuples = calculateCost(node.getBase()); // recursive her
        if (!isFeasible) {
            return Integer.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();

        int index = schema.indexOf(attr);
        // what is the point of this? Isn't attr == fullattr?
        Attribute fullattr = schema.getAttribute(index);

        int exprtype = con.getExprType();

        /* Get number of distinct values of selection attributes **/

        int numdistinct = attrToV.get(fullattr);
        int outtuples;

        /* calculate the number of tuples in result */
        if (exprtype == Condition.EQUAL) {
            outtuples = (int) Math.ceil((double) intuples / (double) numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (int) Math.ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            outtuples = (int) Math.ceil(0.5 * intuples);
        }

        /* Modify the number of distinct values of each attribute, assuming the values are distributed uniformly along entire relation */
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attri = schema.getAttribute(i);
            int oldvalue = attrToV.get(attri);
            int newvalue = (int) Math.ceil(((double) outtuples / (double) intuples) * oldvalue);
            attrToV.put(attri, outtuples);
        }
        return outtuples;
    }

    /**
     * the statistics file <tablename>.stat to find the statistics about that table;
     * This table contains number of tuples in the table and the number of distinct values of each attribute
     **/
    protected int getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat"; // this is where the table meta-data is stored
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }

        int numtuples = getNumTuples(in);
        mapAttrToV(schema, numAttr, in);

        int tupleSizeInBytes = schema.getTupleSize();
        int numTuplesPerPage = Batch.getPageSize() / tupleSizeInBytes;
        int numpages = (int) Math.ceil((double) numtuples / (double) numTuplesPerPage);
        cost += numpages;
        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }

//        Debug.printPurple("\nStatistics for table " + tablename + ": numTuples = " + numtuples + " tuple size = " + tupleSizeInBytes + " number of pages = " + numpages);
        return numtuples;
    }

    private void mapAttrToV(Schema schema, int numAttr, BufferedReader in) {
        StringTokenizer tokenizer;
        String temp;

        String line = null;
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line");
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file");
            System.exit(1);
        }

        for (int i = 0; i < numAttr; i++) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Integer distinctValues = Integer.valueOf(temp);
            attrToV.put(attr, distinctValues);
        }
    }

    private int getNumTuples(BufferedReader in) {
        String lineA = null;

        // First line = number of tuples
        try {
            lineA = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in reading first line");
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(lineA);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statistics file");
            System.exit(1);
        }

        String temp = tokenizer.nextToken();
        /* number of tuples in this table; */
        return Integer.parseInt(temp);
    }

    /**
     * @param num x
     * @param base log base
     * @return ceiling of log(x) base the number given.
     */
    private int getCeilLog(int num, int base) {
        int ans = 0;
        while (num > 0) {
            num /= base;
            ans++;
        }
        return ans;
    }
}