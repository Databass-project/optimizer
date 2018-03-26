
/**
 * This is base class for all the operators
 **/

package qp.operators;

import qp.utils.*;

public class Operator {
    int optype;   //Whether it is OpType.SELECT/ Optype.PROJECT/OpType.JOIN
    Schema schema;   // Schema of the result at this operator

    public Operator(int type) {
        this.optype = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schm) {
        this.schema = schm;
    }

    public void setOpType(int type) {
        this.optype = type;
    }

    public int getOpType() {
        return optype;
    }

    public boolean open() {
        return true;
    }

    public Batch next() {
        System.out.println("Operator:  ");
        return null;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        return new Operator(optype);
    }

}










