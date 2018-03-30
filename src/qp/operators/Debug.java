package qp.operators;

import qp.utils.*;

public class Debug {
    /* print the attribute **/
    public static void PPrint(Attribute attr) {
        String tabname = attr.getTabName();
        String colname = attr.getColName();
        System.out.print(tabname + "." + colname);
    }

    /** print the condition **/
    public static void PPrint(Condition con) {
        Attribute lhs = con.getLhs();
        Object rhs = con.getRhs();
        int exprtype = con.getExprType();
        PPrint(lhs);
        switch (exprtype) {
            case Condition.LESSTHAN:
                System.out.print("<");
                break;
            case Condition.GREATERTHAN:
                System.out.print(">");
                break;
            case Condition.LTOE:
                System.out.print("<=");
                break;
            case Condition.GTOE:
                System.out.print(">=");
                break;
            case Condition.EQUAL:
                System.out.print("==");
                break;
            case Condition.NOTEQUAL:
                System.out.print("!=");
                break;
        }

        if (con.getOpType() == Condition.JOIN) {
            PPrint((Attribute) rhs);
        } else if (con.getOpType() == Condition.SELECT) {
            System.out.print((String) rhs);
        }
    }


    /** print schema **/

    public static void PPrint(Schema schema) {
        System.out.println();
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attr = schema.getAttribute(i);
            PPrint(attr);
        }
        System.out.println();
    }


    /** print a node in plan tree **/

    public static void PPrint(Operator node) {
        int optype = node.getOpType();

        if (optype == OpType.JOIN) {
            int exprtype = ((Join) node).getJoinType();
            switch (exprtype) {
                case JoinType.NESTEDJOIN:
                    System.out.print("NestedJoin(");
                    break;
                case JoinType.BLOCKNESTED:
                    System.out.print("BlockNested(");
                    break;
                case JoinType.SORTMERGE:
                    System.out.print("SortMerge(");
                    break;
                case JoinType.HASHJOIN:
                    System.out.print("HashJoin(");
                    break;
            }
            PPrint(((Join) node).getLeft());
            System.out.print("  [");
            PPrint(((Join) node).getCondition());
            System.out.print("]  ");
            PPrint(((Join) node).getRight());
            System.out.print(")");

        } else if (optype == OpType.SELECT) {
            System.out.print("Select(");
            PPrint(((Select) node).getBase());
            System.out.print("  '");
            PPrint(((Select) node).getCondition());
            System.out.print("'  ");
            System.out.print(")");

        } else if (optype == OpType.PROJECT) {
            System.out.print("Project(");
            PPrint(((Project) node).getBase());
            System.out.print(")");

        } else if (optype == OpType.SCAN) {
            System.out.print(((Scan) node).getTabName());
        }
    }


    /** print a tuple **/

    public static void PPrint(Tuple t) {
        for (int i = 0; i < t.data().size(); i++) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                System.out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                System.out.print(((Float) data).floatValue() + "\t");
            } else {
                System.out.print(((String) data) + "\t");
            }
        }
        System.out.println();
    }


    /**print a page of tuples **/

    public static void PPrint(Batch b) {
        for (int i = 0; i < b.size(); i++) {
            PPrint(b.elementAt(i));
            System.out.println();
        }
    }

    public static void printWithLines(String str) {
        System.out.println("--------------------" + str + "--------------------");
    }

}