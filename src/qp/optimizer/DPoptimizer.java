package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;

import java.util.*;

/**
 * Dynamic Programming optimizer that only considers left-deep trees WITHOUT cartesian products.
 * Note that if we consider CP then the #states = 2^k - 1.
 */
public class DPoptimizer {
    private SQLQuery query;
    private int numJoins;
    private boolean hasOrderBy;

    /**
     * maps plans to actual operator
     */
    private Vector<Condition> joinConditions;

    /**
     * contains the mapping from a set of conditions to the cost of the optimal tree which consists of the conditions in the set
     */
    private HashMap<HashSet<Condition>, Integer> costMap = new HashMap<>();

    /**
     * contains the mapping from a set of conditions to the operator tree with minimum cost.
     */
    private HashMap<HashSet<Condition>, Operator> operatorMap = new HashMap<>();

    /**
     * contains the mapping from a set of conditions to the tables it contains.
     */
    private HashMap<HashSet<Condition>, HashSet<String>> tableMap = new HashMap<>();

    private OperatorUtils util;

    private boolean DEBUG = false;

    public DPoptimizer(SQLQuery query) {
        this.query = query;
        joinConditions = (Vector<Condition>) query.getJoinList();
        numJoins = this.query.getNumJoin();
        if (query.getOrderByList() != null && query.getOrderByList().size() > 0)
            hasOrderBy = true;

        util = new OperatorUtils(this.query);
        computeSingleRelationPlan();
        if (numJoins > 0)
            computeBaseJoinRelationPlan();
    }

    /**
     * @return the logical root of the operator tree with minimum cost
     */
    public Operator getBestPlan() {
        if (numJoins == 0) {
            if (hasOrderBy) {
                util.createOrderByOp();
            }
            util.createProjectOp();
            return util.getRoot();
        }

        for (int cardinality = 2; cardinality <= numJoins; cardinality++) {
            if (DEBUG)
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
//                    int cost = new PlanCost().getCost(newJoin);
                    int cost = tryEachJoinMethod(newJoin);
                    if (!newCostMap.containsKey(newTree) || newCostMap.get(newTree) > cost) {
                        newCostMap.put(newTree, cost);
                        newOperatorMap.put(newTree, newJoin);
                        String rightTableName = ((Attribute)c.getRhs()).getTabName();
//                        tableMap.get(tree).add(rightTableName); // culprit. If there are more than one join conditions that this tree can join with, then it will add + 1 table every time.
                        HashSet<String> newTableNames = new HashSet<>(tableMap.get(tree));
                        newTableNames.add(rightTableName);
                        newTableMap.put(newTree, newTableNames);
                        if (DEBUG) {
                            Debug.printRed("\nUpdate is taking place for joins containing ");
                            Debug.printHashSet(newTableMap.get(newTree));
                            Debug.printRed("with cost = " + cost + "\n");
                        }
                    } else if (DEBUG){
                        Debug.printRed("\n" + "Better solution exists with cost = " + newCostMap.get(newTree) + " than this cost = " + cost + " for ");
                        Debug.printHashSet(newTableMap.get(newTree));
                        System.out.println();
                    }
                }
            }
            // swap the costMap, tableMap and operatorMap such that they only contain elements whose
            // size == cardinality
            costMap = newCostMap;
            operatorMap = newOperatorMap;
            tableMap = newTableMap;

        } // end loop

        if (operatorMap.size() == 1) {
            if (DEBUG)
                Debug.printBold("\nThe operatorMap contains a unique operator tree");
            Operator bestTree = operatorMap.values().iterator().next();
            if (hasOrderBy)
                bestTree = createOrderByOp(bestTree);
            return createProjectOp(bestTree);
        } else {
            if (DEBUG)
                Debug.printBold("\nThe operatorMap contains more than one operator tree");
            return null;
        }
    }

    private void printCurrentOperators(Collection<Operator> trees) {
        int i = 0;
        for (Operator tree: trees) {
            Debug.printWithLines(true, "");
            Debug.printBold("Tree #" + ++i);
            System.out.println(getTreeRepresentation(tree));
        }
    }

    /**
     * @param tree a set of join conditions that make up a subtree
     * @return a list of join conditions which can be added to grow this subtree
     */
    private ArrayList<Condition> getPossibleJoinConditions(HashSet<Condition> tree) {
        Debug.printWithLines(true, "");
        if (DEBUG) {
            Debug.printHashSet(tree);
            System.out.println("\nthe tree contains these tables");
            Debug.printHashSet(tableMap.get(tree));
        }
        ArrayList<Condition> possibleJoinConditions = new ArrayList<>();

        for (int i = 0; i < numJoins; i++) {
            Condition joinCondition = joinConditions.get(i);
            // check this join has not been applied to this tree yet
            if (!tree.contains(joinCondition)) {
                // check if it can be joined
                if (DEBUG) {
                    System.out.println("\nChecking against the following condition");
                    Debug.PPrint(joinCondition);
                }

                String rightTableName = ((Attribute)joinCondition.getRhs()).getTabName();
                String leftTableName = joinCondition.getLhs().getTabName();
                if (tableMap.get(tree).contains(rightTableName)) {
                    joinCondition.flip();
                    Condition clone = (Condition) joinCondition.clone();
                    possibleJoinConditions.add(clone);
                    if (DEBUG)
                        Debug.printRed(" <-- adding this condition\n");
                } else if (tableMap.get(tree).contains(leftTableName)) {
                    Condition clone = (Condition) joinCondition.clone();
                    possibleJoinConditions.add(clone);
                    if (DEBUG)
                        Debug.printRed(" <-- adding this condition\n");
                } else if (DEBUG){
                    System.out.println();
                }
            }
        }

        // this should return at least one: otherwise, we cannot grow the subtree to produce the full result
        return possibleJoinConditions;
    }

    private void computeSingleRelationPlan() {
        // at this stage, it is just SCAN (and SELECT) operation
        util.createScanOp();
        util.createSelectOp();
    }

    /**
     * This method creates two-table joins later to be used as the starting point for the Dynamic Programming in getBestPlan.
     */
    private void computeBaseJoinRelationPlan() {
        for (Condition cOriginal: joinConditions) {
            Condition c = (Condition) cOriginal.clone();
            Operator rightOp = util.getOperator(((Attribute)c.getRhs()).getTabName());
            Operator leftOp = util.getOperator((c.getLhs()).getTabName());
            Operator join = new Join(leftOp, rightOp, c, OpType.JOIN);
            Schema jointSchema = leftOp.getSchema().joinWith(rightOp.getSchema());
            join.setSchema(jointSchema);
            if (DEBUG) {
                Debug.printWithLines(false, "");
                Debug.printBold("Calculating the cost of join without flipping");
            }
//            int cost = new PlanCost().getCost(join);
            int cost = tryEachJoinMethod(join);

            /// now flip the RHS and LHS of the join
            c.flip();
            Operator flippedJoin = new Join(rightOp, leftOp, c, OpType.JOIN);
            Schema jointSchemaFlipped = rightOp.getSchema().joinWith(leftOp.getSchema());
            flippedJoin.setSchema(jointSchemaFlipped);
            if (DEBUG)
                Debug.printBold("Calculating the cost of join after flipping");
//            int costFlippedJoin =  new PlanCost().getCost(flippedJoin);
            int costFlippedJoin = tryEachJoinMethod(flippedJoin);
            HashSet<Condition> hs = new HashSet<>();

            if (DEBUG)
                System.out.println("\ncomputeBaseJoinRelationPlan: CostMap contains ");
            if (cost < costFlippedJoin) {
                c.flip(); // flip it back
                hs.add(c);
                costMap.put(hs, cost);
                operatorMap.put(hs, join);
                if (DEBUG)
                    Debug.PPrint(join);
            } else {
                hs.add(c);
                costMap.put(hs, costFlippedJoin);
                operatorMap.put(hs, flippedJoin);
                if (DEBUG) {
                    Debug.PPrint(flippedJoin);
                    Debug.printRed(" <--- flipped.");
                }
            }
            if (DEBUG)
                Debug.printBold(" cost == " + cost + " vs. flipped cost == " + costFlippedJoin);
            HashSet<String> tableNames = new HashSet<>();
            tableNames.add(c.getLhs().getTabName());
            tableNames.add(((Attribute)c.getRhs()).getTabName());
            tableMap.put(hs, tableNames);
        }
    }

    /**
     * Tries different types of join methods on a given join operator to calculate
     * the minimum cost. The join method type of the given operator will be set to the one
     * which yields the minimum cost.
     * @param root root of the tree to calculate the cost
     * @return the cost of the tree with best join method for the top-most join
     */
    private int tryEachJoinMethod(Operator root) {
        if (root.getOpType() != OpType.JOIN) {
            System.exit(1);
        }
        int minCost = Integer.MAX_VALUE;
        int minJoinType = 0;
        for (int type = 0; type < JoinType.numJoinTypes(); type++) {
            ((Join) root).setJoinType(type);
            int currentCost = new PlanCost().getCost(root);
            if (minCost > currentCost) {
                minCost = currentCost;
                minJoinType = type;
            }
        }
        ((Join) root).setJoinType(minJoinType);

        return minCost;
    }

    public Operator createProjectOp(Operator root) { // currently, no push-down of projection
        Vector<Attribute> projectlist = util.getProjectlist();
        Operator base = root;

        if (!projectlist.isEmpty()) {
            root = new Project(base, projectlist, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }

        return root;
    }

    /**
     * Create OrderBy Operator for the attributes mentioned in from list
     **/
    public Operator createOrderByOp(Operator root) {
        Operator base = root;
        root = new OrderBy(base, util.getOrderbyList(), OpType.ORDERBY);
        root.setSchema(base.getSchema());
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

    /** After finding a choice of method for each operator, prepare an execution plan by replacing the methods with corresponding join operator implementation
     */
    public static Operator makeExecPlan(Operator node) {

        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            Join joinOperator;
            switch (joinType) {
                case JoinType.NESTEDJOIN:
                    joinOperator = new NestedJoin((Join) node);
                    break;
                case JoinType.HASHJOIN:
                case JoinType.SORTMERGE:
                    joinOperator = new SortMerge((Join) node);
                    break;
                case JoinType.BLOCKNESTED:
                    joinOperator = new BlockNestedJoin((Join) node);
                    break;
                default:
                    return node;
            }
            joinOperator.setLeft(left);
            joinOperator.setRight(right);
            joinOperator.setNumBuff(numbuff);
            return joinOperator;
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.ORDERBY) {
            OrderBy ob = (OrderBy) node;
            Operator base = makeExecPlan(ob.getBase());
            ob.setBase(base);
            // since we have to materialize the incoming batches anyways, we can have the entire buffer
            // to perform the orderby
            int numbuff = BufferManager.numBuffer;
            ob.setNumBuff(numbuff);
            return ob;
        } else {
            return node;
        }
    }

}
