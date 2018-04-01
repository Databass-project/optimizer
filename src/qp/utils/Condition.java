/**
 * represents join/select condition
 **/


package qp.utils;

import java.util.UUID;
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

//    private static String uniqueID = UUID.randomUUID().toString();
//    protected String ID;
    private static long uniqueID = (int) (Math.random() * 1000000);
    protected long ID;

    public Condition(Attribute attr, int type, Object value) {
        lhs = attr;
        exprtype = type;
        this.rhs = value;
        this.ID = uniqueID;
//        uniqueID = UUID.randomUUID().toString(); // just make it random
        uniqueID = (int) (Math.random() * 1000000);
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

    @Override
    public Object clone() {
        Attribute clonedLHS = (Attribute) lhs.clone();
        Object clonedRHS;

        if (optype == SELECT)
            clonedRHS = rhs;
        else
            clonedRHS = ((Attribute) rhs).clone();
        Condition clonedCn = new Condition(clonedLHS, exprtype, clonedRHS);
        clonedCn.ID = this.ID; // so that we can tell they are equal, even if cloned
        clonedCn.setOpType(optype);
        return clonedCn;
    }

    /*
     * the following is a hack to make clone hash and compare equal to its original condition.
     * Conditions need to be cloned such that we can build different trees in which
     * the condition may be flipped.
     * */
    @Override
//    public int hashCode() {
//        return this.ID.hashCode();
//    }
    public int hashCode() {
        return (int) this.ID;
    }

//    @Override
//    public boolean equals(Object another) {
//        return another instanceof Condition && ((Condition) another).ID.equals(this.ID);
//    }

    @Override
    public boolean equals(Object another) {
        return another instanceof Condition && ((Condition) another).ID == (this.ID);
    }



//    // testing for hashing
//    Condition cClone = (Condition) c.clone();
//            if (cClone.hashCode() == c.hashCode()) {
//        if (cClone.equals(c))
//            System.out.println("Clone hashes to the same value and are equal");
//        else
//            System.out.println("Clone hashes to the same value but not equal");
//    } else {
//        System.out.println("Clone does not hash to the same value");
//    }
//            System.exit(1);
}








