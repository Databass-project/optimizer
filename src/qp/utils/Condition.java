/**
 * represents join/select condition
 **/


package qp.utils;

public class Condition {
    /** enumeration of the op code in the condition **/
    public static final int LESSTHAN = 1;
    public static final int GREATERTHAN = 2;
    public static final int LTOE = 3;
    public static final int GTOE = 4;
    public static final int EQUAL = 5;
    public static final int NOTEQUAL = 6;

    public static final int SELECT = 1;
    public static final int JOIN = 2;

    Attribute lhs;   //left hand side of the condition
    int optype;      // Whether select condition or join condition
    int exprtype;   // Comparision type, equal to/lessthan/greaterthan etc.,
    Object rhs;   // This is Attribute for Join condition and String for Select Condition

    public Condition(Attribute attr, int type, Object value) {
        lhs = attr;
        exprtype = type;
        this.rhs = value;
    }

    public Condition(int type) {
        exprtype = type;
    }

    public Attribute getLhs() {
        return lhs;
    }

    public void setLhs(Attribute attr) {
        lhs = attr;
    }

    public void setOpType(int num) {
        optype = num;
    }

    public int getOpType() {
        return optype;
    }

    public void setExprType(int num) {
        exprtype = num;
    }

    public int getExprType() {
        return exprtype;
    }

    public Object getRhs() {
        return rhs;
    }

    public void setRhs(Object value) {
        rhs = value;
    }

    public void flip() {
        if (optype == JOIN) {
            Object temp = lhs;
            lhs = (Attribute) rhs;
            rhs = temp;
        }
    }

    public Object clone() {
        Attribute newlhs = (Attribute) lhs.clone();
        Object newrhs;

        if (optype == SELECT)
            newrhs = (String) rhs;
        else
            newrhs = (Attribute) ((Attribute) rhs).clone();
        Condition newcn = new Condition(newlhs, exprtype, newrhs);
        newcn.setOpType(optype);
        return newcn;
    }
}








