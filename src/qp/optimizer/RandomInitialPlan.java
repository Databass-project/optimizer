/* prepares a random initial plan for the given SQL query see the ReadMe file to understand this */
package qp.optimizer;
import qp.utils.*;
import qp.operators.*;

import java.util.Vector;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.*;

public class RandomInitialPlan {

    SQLQuery sqlquery;

    /**
     * list of attributes to be projected
     */
    Vector projectlist;
    Vector fromlist;
    Vector selectionlist;
    Vector joinlist;
    Vector orderbylist;
    int numJoin;    // Number of joins in this query

    Hashtable tab_op_hash; //table name to the Operator
    Operator root; // root of the query plan tree

    public RandomInitialPlan(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;

        projectlist = (Vector) sqlquery.getProjectList();
        fromlist = (Vector) sqlquery.getFromList();
        selectionlist = sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        orderbylist = sqlquery.getOrderByList();
        numJoin = joinlist.size();
    }

    /**
     *  @return number of join conditions
     **/
    public int getNumJoins() {
        return numJoin;
    }

    /**
     * @return prepare initial plan for the query
     **/
    public Operator prepareInitialPlan() {
        tab_op_hash = new Hashtable();

        createScanOp();
        createSelectOp();
        if (numJoin != 0) {
            createJoinOp();
        }
        createProjectOp();
        createOrderByOp();
        return root;
    }

    /**
     * Create Scan Operator for each of the table mentioned in from list
     **/
    public void createScanOp() {
        int numTables = fromlist.size();
        Scan tempop = null;

        for (int i = 0; i < numTables; i++) {  // For each table in from list
            String tabname = (String) fromlist.elementAt(i);
            Scan op1 = new Scan(tabname, OpType.SCAN);
            tempop = op1;

            MapTableToOp(tabname, op1);
        }

        // To handle the case where there is no where clause selectionlist is empty, hence we set the root to be the scan operator. the projectOp would be put on top of this later in CreateProjectOp
        if (selectionlist.size() == 0) {
            root = tempop;
        }
    }

    /**
     * Create OrderBy Operator for the attributes mentioned in from list
     **/
    public void createOrderByOp() {
        Operator base = root;
        /* The last selection is the root of the plan tree constructed thus far */
        if (orderbylist.size() != 0){
        	root = new OrderBy(base, orderbylist, OpType.ORDERBY);
        	root.setSchema(base.getSchema());
        }
    }
    
    /**
     * Read the schema of the table from tablename.md file (md = metadata)
     **/
    private void MapTableToOp(String tabname, Scan op1) {
        String filename = tabname + ".md";
        try {
            ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
            Schema schm = (Schema) _if.readObject();
            op1.setSchema(schm);
            _if.close();
        } catch (Exception e) {
            System.err.println("RandomInitialPlan:Error reading Schema of the table: " + filename);
            System.exit(1);
        }
        tab_op_hash.put(tabname, op1);
    }

    /**
     * Create Selection Operators for each of the selection condition mentioned in Condition list
     **/
    public void createSelectOp() {
        Select newOperator = null;

        for (int j = 0; j < selectionlist.size(); j++) {
            Condition cn = (Condition) selectionlist.elementAt(j);
            if (cn.getOpType() == Condition.SELECT) { // the other type is Join
                String tabname = cn.getLhs().getTabName();

                Operator baseOperator = (Operator) tab_op_hash.get(tabname);
                newOperator = new Select(baseOperator, cn, OpType.SELECT);
                /* set the schema same as base relation */
                newOperator.setSchema(baseOperator.getSchema());

                updateHashtable(baseOperator, newOperator);
            }
        }
        /* The last selection is the root of the plan tree constructed thus far */
        if (selectionlist.size() != 0)
            root = newOperator;
    }

    /**
     * create join operators
     **/
    public void createJoinOp() {
        BitSet bitCList = new BitSet(numJoin);
        int jnnum = RandNumb.randInt(0, numJoin - 1);
        Join newJoin = null;
        /* Repeat until all the join conditions are considered */
        while (bitCList.cardinality() != numJoin) {
            /* If this condition is already considered, choose another join condition */
            while (bitCList.get(jnnum)) {
                jnnum = RandNumb.randInt(0, numJoin - 1);
            }
            Condition cn = (Condition) joinlist.elementAt(jnnum);
            String lefttab = cn.getLhs().getTabName(); // this is why you need the table name to be the same
            String righttab = ((Attribute) cn.getRhs()).getTabName();

            Operator left = (Operator) tab_op_hash.get(lefttab);
            Operator right = (Operator) tab_op_hash.get(righttab);
            newJoin = new Join(left, right, cn, OpType.JOIN);
            newJoin.setNodeIndex(jnnum);
            Schema jointSchema = left.getSchema().joinWith(right.getSchema());
            newJoin.setSchema(jointSchema);
            /* randomly select a join type */
            int numJMeth = JoinType.numJoinTypes();
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            newJoin.setJoinType(joinMeth);

            updateHashtable(left, newJoin);
            updateHashtable(right, newJoin);
            bitCList.set(jnnum);
        }
        /* The last join operation is the root for the constructed till now */
        if (numJoin != 0)
            root = newJoin;
    }

    public void createProjectOp() { // currently, no push-down of selection and projection
        Operator base = root;
        if (projectlist == null) // projectlist should normally be set in the constructor
            projectlist = new Vector();

        if (!projectlist.isEmpty()) {
            root = new Project(base, projectlist, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }

    /**
     * @param oldOp older operator which is a value in hashmap
     * @param newOp new operator to replace the key-value mapping in place of old operator
     */
    private void updateHashtable(Operator oldOp, Operator newOp) {
        Enumeration e = tab_op_hash.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            Operator temp = (Operator) tab_op_hash.get(key);
            if (temp == oldOp) {
                tab_op_hash.put(key, newOp);
            }
        }
    }

}