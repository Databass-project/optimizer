/* prepares a random initial plan for the given SQL query see the ReadMe file to understand this */
package qp.optimizer;
import qp.utils.*;
import qp.operators.*;

import java.util.Vector;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.*;

public class OperatorUtils {
    private SQLQuery sqlquery;

    /**
     * list of attributes to be projected
     */
    private Vector<Attribute> projectlist;
    private Vector<String> fromlist;
    private Vector<Condition> selectionlist;
    private Vector<Condition> joinlist;
    private Vector<Attribute> orderbyList;
    private int numJoin;    // Number of joins in this query

    private Hashtable<String, Operator> tableNameToOperator = new Hashtable<>();
    private Operator root; // root of the query plan tree

    public OperatorUtils(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;

        projectlist = (Vector<Attribute>) sqlquery.getProjectList();
        fromlist = (Vector<String>) sqlquery.getFromList();
        selectionlist = (Vector<Condition>) sqlquery.getSelectionList();
        joinlist = (Vector<Condition>) sqlquery.getJoinList();
        orderbyList = sqlquery.getOrderByList();
        numJoin = joinlist.size();
    }

    public Operator getRoot() {
        return this.root;
    }

    /**
     *  @return number of join conditions
     **/
    public int getNumJoins() {
        return numJoin;
    }

    public Operator getOperator(String tableName) {
        return tableNameToOperator.get(tableName);
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
            System.err.println("OperatorUtils :Error reading Schema of the table: " + filename);
            System.exit(1);
        }
        tableNameToOperator.put(tabname, op1);
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

                Operator baseOperator = (Operator) tableNameToOperator.get(tabname);
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
        Enumeration e = tableNameToOperator.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            Operator temp = (Operator) tableNameToOperator.get(key);
            if (temp == oldOp) {
                tableNameToOperator.put(key, newOp);
            }
        }
    }

    /**
     * Create OrderBy Operator for the attributes mentioned in from list
     **/
    public void createOrderByOp() {
        Operator base = root;
        /* The last selection is the root of the plan tree constructed thus far */
        if (orderbyList.size() != 0){
            root = new OrderBy(base, orderbyList, OpType.ORDERBY);
            root.setSchema(base.getSchema());
        }
    }

    public Vector<Attribute> getProjectlist() {
        return this.projectlist;
    }

    public Vector<Attribute> getOrderbyList() { return this.orderbyList; }

}