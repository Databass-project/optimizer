package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;

import java.util.*;

/**
 * Dynamic Programming optimizer that only considers left-deep trees without cartesian products.
 * Note that if we consider CP then the #states = 2^k - 1.
 */
public class DPoptimizer {
    private SQLQuery query;
    private int numJoins;
    private boolean DEBUG = true;

    /**
     * maps plans to actual operator
     */
    private Vector<Condition> joinConditions;
    private HashMap<HashSet<Condition>, Integer> costMap = new HashMap<>();
    private HashMap<HashSet<Condition>, Operator> operatorMap = new HashMap<>();
    private HashMap<HashSet<Condition>, HashSet<String>> tableMap = new HashMap<>();

    private OperatorUtils util;

    /* we are going to avoid Cartesian products, since they can be harmful in most cases */
    public DPoptimizer(SQLQuery query) {
        this.query = query;
        joinConditions = (Vector<Condition>) query.getJoinList();
        numJoins = this.query.getNumJoin();
        ; // need to use string, not operator as key: otherwise, how
        // do you retrieve the value? This string should not have ordering

        // the two tables you are joining must contain common attributes in the schema
        // the first two tables at the leaf defines more or less what kind of tree you are going to get.

        util = new OperatorUtils(this.query);
        computeSingleRelationPlan();
        if (numJoins > 0)
            computeJoinRelationPlan();
    }

    public Operator getBestPlan() {
        // case of no joins to perform
        if (numJoins == 0) {
            util.createProjectOp();
            return util.getRoot();
        }

        for (int cardinality = 2; cardinality <= numJoins; cardinality++) {
            // for each small join tree, traverse through the join conditions
            // if the join tree contains k tables, it can combine with at most k join conditions
            Debug.printBold("\n\ncardinality = " + cardinality + ": " + costMap.size() + " subtrees");
            HashMap<HashSet<Condition>, Integer> newCostMap = new HashMap<>();
            HashMap<HashSet<Condition>, Operator> newOperatorMap = new HashMap<>();
            HashMap<HashSet<Condition>, HashSet<String>> newTableMap = new HashMap<>();
            for (HashSet<Condition> tree: costMap.keySet()) {
                ArrayList<Condition> possibleConditions = getPossibleJoinConditions(tree);
                for (Condition c: possibleConditions) {
                    // make new left-deep tree, and compute the cost of it
                    Operator rightTable = util.getOperator(((Attribute) c.getRhs()).getTabName());
                    Join newJoin = new Join(operatorMap.get(tree), rightTable, c, OpType.JOIN);

                    Schema jointSchema = operatorMap.get(tree).getSchema().joinWith(rightTable.getSchema());
                    newJoin.setSchema(jointSchema);

                    HashSet<Condition> newTree = new HashSet<>(tree);
                    newTree.add(c);
                    int cost = new PlanCost().getCost(newJoin);
                    if (!newCostMap.containsKey(newTree) || newCostMap.get(newTree) > cost) {
                        newCostMap.put(newTree, cost);
                        newOperatorMap.put(newTree, newJoin);
                        String rightTableName = ((Attribute)c.getRhs()).getTabName();
//                        tableMap.get(tree).add(rightTableName); // culprit. If there are more than one join conditions that this tree can join with, then it will add + 1 table every time.
                        HashSet<String> newTableNames = new HashSet<>(tableMap.get(tree));
                        newTableNames.add(rightTableName);
                        newTableMap.put(newTree, newTableNames);
                        Debug.printRed("\nUpdate is taking place for joins containing ");
                        Debug.printHashSet(newTableMap.get(newTree));
                        Debug.printRed("with cost = " + cost +"\n");
                    } else {
                        Debug.printRed("\n" + "Better solution exists with cost = " + newCostMap.get(newTree) + " than this cost = " + cost + " for ");
                        Debug.printHashSet(newTableMap.get(newTree));
                        System.out.println();
                    }
                }
            }
            // swap the costMap and operatorMap such that they only contain elements whose
            // size == cardinality
            costMap = newCostMap;
            operatorMap = newOperatorMap;
            printCurrentOperators(operatorMap.values());
            tableMap = newTableMap;
        }

        if (operatorMap.size() == 1) {
            Debug.printBold("\nThe operatorMap contains a unique operator tree");
            Operator bestTree = operatorMap.values().iterator().next();
            return createProjectOp(bestTree);
        }
        Debug.printBold("\nThe operatorMap contains more than one operator tree");
        return null;
    }

    private void printCurrentOperators(Collection<Operator> trees) {
        int i = 0;
        for (Operator tree: trees) {
            Debug.printWithLines(true, "");
            Debug.printBold("Tree #" + ++i);
            System.out.println(getTreeRepresentation(tree));
        }
    }

    private ArrayList<Condition> getPossibleJoinConditions(HashSet<Condition> tree) {
        Debug.printWithLines(true, "");
        Debug.printHashSet(tree);
        ArrayList<Condition> possibleJoinConditions = new ArrayList<>();
        System.out.println("\nthe tree contains these tables");
        Debug.printHashSet(tableMap.get(tree));

        for (int i = 0; i < numJoins; i++) {
            Condition joinCondition = joinConditions.get(i);
            // check this join has not been applied to this tree yet
            if (!tree.contains(joinCondition)) {
                // check if it can be joined
                System.out.println("\nChecking against the following condition (not flipped yet)");
                Debug.PPrint(joinCondition);

                String rightTableName = ((Attribute)joinCondition.getRhs()).getTabName();
                String leftTableName = joinCondition.getLhs().getTabName();
                if (tableMap.get(tree).contains(rightTableName)) {
                    joinCondition.flip();
                    Condition clone = (Condition) joinCondition.clone();
                    possibleJoinConditions.add(clone);
                    Debug.printRed(" <-- adding this condition\n");
                } else if (tableMap.get(tree).contains(leftTableName)) {
                    Condition clone = (Condition) joinCondition.clone();
                    possibleJoinConditions.add(clone);
                    Debug.printRed(" <-- adding this condition\n");
                } else {
                    System.out.println();
                }
            }
        }

        // this should return at least one: otherwise, you cannot produce the full result
        return possibleJoinConditions;
    }

    private void computeSingleRelationPlan() {
        // at this stage, it is just SCAN (and SELECT) operation
        util.createScanOp();
        util.createSelectOp();
    }

    private void computeJoinRelationPlan() {
        for (Condition cOriginal: joinConditions) {
            Condition c = (Condition) cOriginal.clone();
            Operator rightOp = util.getOperator(((Attribute)c.getRhs()).getTabName());
            Operator leftOp = util.getOperator((c.getLhs()).getTabName());
            Operator join = new Join(leftOp, rightOp, c, OpType.JOIN);
            Schema jointSchema = leftOp.getSchema().joinWith(rightOp.getSchema());
            join.setSchema(jointSchema);
            Debug.printWithLines(false, "");
            Debug.printBold("Calculating the cost of join without flipping");
            PlanCost pc = new PlanCost();
            int cost = pc.getCost(join);

            /// now flip the RHS and LHS of the join
            c.flip();
            Operator flippedJoin = new Join(rightOp, leftOp, c, OpType.JOIN);
            Schema jointSchemaFlipped = rightOp.getSchema().joinWith(leftOp.getSchema());
            flippedJoin.setSchema(jointSchemaFlipped);
            Debug.printBold("Calculating the cost of join after flipping");
            int costFlippedJoin =  new PlanCost().getCost(flippedJoin);
            HashSet<Condition> hs = new HashSet<>();

            System.out.println("\ncomputeJoinRelationPlan: CostMap contains ");
            if (cost < costFlippedJoin) {
                c.flip(); // flip it back
                hs.add(c);
                costMap.put(hs, cost);
                operatorMap.put(hs, join);
                Debug.PPrint(join);
            } else {
                hs.add(c);
                costMap.put(hs, costFlippedJoin);
                operatorMap.put(hs, flippedJoin);
                Debug.PPrint(flippedJoin);
                Debug.printRed(" <--- flipped.");
            }
            Debug.printBold(" cost == " + cost + " vs. flipped cost == " + costFlippedJoin);
            HashSet<String> tableNames = new HashSet<>();
            tableNames.add(c.getLhs().getTabName());
            tableNames.add(((Attribute)c.getRhs()).getTabName());
            tableMap.put(hs, tableNames);
        }
    }

    public Operator createProjectOp(Operator root) { // currently, no push-down of selection and projection
        Vector<Attribute> projectlist = util.getProjectlist();
        Operator base = root;

        if (!projectlist.isEmpty()) {
            root = new Project(base, projectlist, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }

        return root;
    }

    public static String getTreeRepresentation(Operator root) {
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
        } else if (root instanceof Select) {
            return ("s( " + getTreeRepresentation(((Select) root).getBase()) + " )");
        } else if (root instanceof Project) {
            return ("p( " + getTreeRepresentation(((Project) root).getBase()) + " )");
        }
        return "";
    }

}
