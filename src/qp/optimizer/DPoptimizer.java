package qp.optimizer;

import qp.operators.Join;
import qp.operators.Operator;
import qp.operators.Scan;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class DPoptimizer {
    private SQLQuery query;
    private int numJoins;

    /**
     * maps plans to actual operator
     */
    private Vector<Condition> joinConditions;
    private HashMap<Set<String>, Integer> costMap = new HashMap<>();
    private HashMap<Set<String>, Operator> operatorMap = new HashMap<>();

    /* we are going to avoid Cartesian products, since they can be harmful in most cases */
    public DPoptimizer(SQLQuery query) {
        this.query = query;
        numJoins = this.query.getNumJoin();
        ; // need to use string, not operator as key: otherwise, how
        // do you retrieve the value? This string should not have ordering

        // the two tables you are joining must contain common attributes in the schema
        // the first two tables at the leaf defines more or less what kind of tree you are going to get.

        computeSingleRelationPlan();

    }

    private void getBestPlan() {
        int numJoinsTaken = 0;
        boolean[] isJoinTaken = new boolean[joinConditions.size()];
        //getBestPlan(operatorMap, costMap, isJoinTaken, numJoinsTaken);
    }

    private void getBestPlan(HashMap<Set<String>, Operator> operatorMap, HashMap<Set<String>, Integer> costMap, boolean[] isJoinTaken, int numJoinsTaken, Operator op) {

        // bottom-up
        for (int i = 0; i < joinConditions.size(); i++) {

        }
        // pick a join condition
        //Condition jc = chooseJoinCondition(,isJoinTaken);
        //String tableToAdd =  ((Attribute) jc.getRhs()).getTabName();


    }

    /**
     * @return another join condition that is not yet chosen and contains a common attribute
     * as left-table attribute with the already-joint table. If no such join condition exists, returns null
     */
    private Condition chooseJoinCondition(Schema currentTable, boolean[] isJoinTaken) {
        for (int i = 0; i < numJoins; i++) {
            if (!isJoinTaken[i]) {
                Condition joinCondition = joinConditions.get(i);
                if (currentTable.contains(joinCondition.getLhs()))
                    return joinCondition;
            }
        }
        return null;
    }

    private void computeSingleRelationPlan() {
        // at this stage, it is just SCAN (and SELECT) operation
        for (Object table: query.getFromList()) {
            PlanCost pc = new PlanCost();
            OperatorUtils util = new OperatorUtils(this.query);
            util.createScanOp();
            util.createSelectOp();
            // get the thing for each table
            String tableName = (String) table;

            Operator tableOp = util.getOperator(tableName);
            //operatorMap.put(new SettableName, tableOp);
            //costMap.put(tableName, pc.getCost(tableOp));
        }
    }

    public String getTreeRepresentation(Operator root) {
        if (root == null) // just to be safe
            return "";
        else if (root instanceof Join) {
            StringBuilder sb = new StringBuilder();
            sb.append(" (");
            sb.append(getTreeRepresentation(((Join) root).getLeft()));
            sb.append(" j ");
            sb.append(getTreeRepresentation(((Join) root).getRight()));
            sb.append(") ");
            return sb.toString();
        } else if (root instanceof Scan) {
            return ((Scan) root).getTabName();
        }
        return "";
    }

}
