/* This is base class for all the join operators */
package qp.operators;

import qp.utils.*;

import java.util.Vector;

public class Join extends Operator {

    Operator left;   // left child
    Operator right;   // right child
    Condition con;     //join condition
    int numBuff;    // Number of buffers available

    int jointype;  // JoinType.NestedJoin/SortMerge/HashJoin
    int nodeIndex;   // Each join node is given a number

    public Join(Operator left, Operator right, Condition cn, int type) {
        super(type);
        this.left = left;
        this.right = right;
        this.con = cn;
    }

    /* number of buffers available to this join operator */

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return numBuff;
    }

    /* index of this node in query plan tree */

    public int getNodeIndex() {
        return nodeIndex;
    }

    public void setNodeIndex(int num) {
        this.nodeIndex = num;
    }

    public int getJoinType() {
        return jointype;
    }

    /* type of join */

    public void setJoinType(int type) {
        this.jointype = type;
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

    public void setCondition(Condition cond) {
        this.con = cond;
    }

    public Condition getCondition() {
        return con;
    }

    public Object clone() {
        Operator newleft = (Operator) left.clone();
        Operator newright = (Operator) right.clone();
        Condition newcond = (Condition) con.clone();

        Join jn = new Join(newleft, newright, newcond, optype);
        Schema newsche = newleft.getSchema().joinWith(newright.getSchema());
        jn.setSchema(newsche);
        jn.setJoinType(jointype);
        jn.setNodeIndex(nodeIndex);
        jn.setNumBuff(numBuff);
        return jn;
    }

}