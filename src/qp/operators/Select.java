/** Select Operation **/

package qp.operators;

import qp.utils.*;
import java.util.Vector;

public class Select extends Operator{

    Operator base;  // base operator
      Condition con; //select condition
	int batchsize;  // number of tuples per outbatch

    /** The following fields are required during
     ** execution of the select operator
     **/

    boolean eos;  // Indiacate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer


	/** constructor **/

    public Select(Operator base, Condition con, int type){
	super(type);
	this.base=base;
	this.con=con;

    }

    public void setBase(Operator base){
	this.base = base;
    }

    public Operator getBase(){
	return base;
    }

    public void setCondition(Condition cn){
	this.con=cn;
    }

    public Condition getCondition(){
	return con;
    }


    /** Opens the connection to the base operator
     **/

    public boolean open(){
	eos=false;     // Since the stream is just opened
	start = 0;   // set the cursor to starting position in input buffer

	/** set number of tuples per page**/
	int tuplesize=schema.getTupleSize();
	batchsize=Batch.getPageSize()/tuplesize;


	if(base.open())
	    return true;
	else
	    return false;
    }


    /** returns a batch of tuples that satisfies the
     ** condition specified on the tuples coming from base operator
     ** NOTE: This operation is performed on the fly
     **/

    public Batch next(){
	//System.out.println("Select:-----------------in next--------------");

	int i=0;

	if(eos){
		close();
	    return null;
	}

        /** An output buffer is initiated**/
	outbatch = new Batch(batchsize);

	/** keep on checking the incoming pages until
	 ** the output buffer is full
	 **/
	while(!outbatch.isFull()){
	    if(start==0){
		inbatch= base.next();
		/** There is no more incoming pages from base operator **/
		if(inbatch==null){

		    eos = true;
		    return outbatch;
		}
	    }

	    /** Continue this for loop until this page is fully observed
	     ** or the output buffer is full
	     **/

	    for(i=start;i<inbatch.size() && (!outbatch.isFull());i++){
		Tuple present = inbatch.elementAt(i);
		/** If the condition is satisfied then
		 ** this tuple is added tot he output buffer
		 **/
		if(checkCondition(present))
		//if(present.checkCondn(con))
		    outbatch.add(present);
	    }

	/** Modify the cursor to the position requierd
	 ** when the base operator is called next time;
	 **/

	    if(i==inbatch.size())
		start=0;
	    else
		start = i;

	    //  return outbatch;
	}
	return outbatch;
    }


    /** closes the output connection
     ** i.e., no more pages to output
     **/

    public boolean close(){
	/**
	if(base.close())
	    return true;
	else
	  return false;
	  **/
	  return true;
    }



	/** To check whether the selection condition is satisfied for
		the present tuple
		**/

    protected boolean checkCondition(Tuple tuple){
	Attribute attr = con.getLhs();
	int index = schema.indexOf(attr);
	int datatype = schema.typeOf(attr);
	Object srcValue = tuple.dataAt(index);
	String checkValue =(String) con.getRhs();
	int exprtype = con.getExprType();

	if(datatype == Attribute.INT){
	    int srcVal = ((Integer)srcValue).intValue();
	    int checkVal = Integer.parseInt(checkValue);
	    if(exprtype==Condition.LESSTHAN){
		if(srcVal < checkVal)
		    return true;
	    }else if(exprtype==Condition.GREATERTHAN){
		if(srcVal>checkVal)
		    return true;
	    }else if(exprtype==Condition.LTOE){
		if(srcVal<= checkVal)
		    return true;
	    }else if(exprtype==Condition.GTOE){
		if(srcVal>=checkVal)
		    return true;
	    }else if(exprtype==Condition.EQUAL){
		if(srcVal==checkVal)
		    return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(srcVal != checkVal)
		    return true;
	    }else{
		System.out.println("Select:Incorrect condition operator");
	    }
	}else if(datatype==Attribute.STRING){
	    String srcVal = (String)srcValue;
	    int flag = srcVal.compareTo(checkValue);

	    if(exprtype==Condition.LESSTHAN){
		if(flag<0) return true;

	    }else if(exprtype==Condition.GREATERTHAN){
		if(flag>0) return true;
	    }else if(exprtype==Condition.LTOE){
		if(flag<=0) return true;
	    }else if(exprtype==Condition.GTOE){
		if(flag>=0) return true;
	    }else if(exprtype==Condition.EQUAL){
		if(flag ==0) return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(flag !=0) return true;
	    }else{
		System.out.println("Select: Incorrect condition operator");
	    }

	}else if(datatype==Attribute.REAL){
	    float srcVal = ((Float) srcValue).floatValue();
	    float checkVal = Float.parseFloat(checkValue);
	    if(exprtype==Condition.LESSTHAN){
		if(srcVal<checkVal) return true;
	    }else if(exprtype==Condition.GREATERTHAN){
		if(srcVal>checkVal) return true;
	    }else if(exprtype==Condition.LTOE){
		if(srcVal<=checkVal) return true;
	    }else if(exprtype==Condition.GTOE){
		if(srcVal>=checkVal) return true;
	    }else if(exprtype==Condition.EQUAL){
		if(srcVal==checkVal) return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(srcVal!=checkVal) return true;
	    }else{
		System.out.println("Select:Incorrect condition operator");
	    }
	}
	return false;
    }






    public Object clone(){
	Operator newbase = (Operator) base.clone();
	Condition newcon = (Condition) con.clone();
	Select newsel = new Select(newbase,newcon,optype);
	newsel.setSchema(newbase.getSchema());
	return newsel;
    }
}



