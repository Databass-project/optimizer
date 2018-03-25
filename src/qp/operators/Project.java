/** To projec out the required attributes from the result **/

package qp.operators;

import qp.utils.*;
import java.util.Vector;

public class Project extends Operator{

    Operator base;
    Vector attrSet;
	int batchsize;  // number of tuples per outbatch


    /** The following fields are requied during execution
     ** of the Project Operator
     **/

    Batch inbatch;
    Batch outbatch;

    /** index of the attributes in the base operator
     ** that are to be projected
     **/

    int[] attrIndex;


    public Project(Operator base, Vector as,int type){
	super(type);
	this.base=base;
	this.attrSet=as;

    }

    public void setBase(Operator base){
	this.base = base;
    }

    public Operator getBase(){
	return base;
    }

    public Vector getProjAttr(){
	return attrSet;
    }

    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open(){
	/** setnumber of tuples per batch **/
	int tuplesize = schema.getTupleSize();
	batchsize=Batch.getPageSize()/tuplesize;


	/** The followingl loop findouts the index of the columns that
	 ** are required from the base operator
	 **/

	Schema baseSchema = base.getSchema();
	attrIndex = new int[attrSet.size()];
	//System.out.println("Project---Schema: ----------in open-----------");
	//System.out.println("base Schema---------------");
	//Debug.PPrint(baseSchema);
	for(int i=0;i<attrSet.size();i++){
	    Attribute attr = (Attribute) attrSet.elementAt(i);
  	    int index = baseSchema.indexOf(attr);
	    attrIndex[i]=index;

	    //  Debug.PPrint(attr);
	    //System.out.println("  "+index+"  ");
	}

	if(base.open())
	    return true;
	else
	    return false;
    }

    /** Read next tuple from operator */

    public Batch next(){
	//System.out.println("Project:-----------------in next-----------------");
	outbatch = new Batch(batchsize);

	/** all the tuples in the inbuffer goes to the output
	    buffer
	**/

	inbatch = base.next();
	// System.out.println("Project:-------------- inside the next---------------");


	if(inbatch == null){
	    return null;
	}
	//System.out.println("Project:---------------base tuples---------");
	for(int i=0;i<inbatch.size();i++){
	    Tuple basetuple = inbatch.elementAt(i);
	    //Debug.PPrint(basetuple);
	    //System.out.println();
	    Vector present = new Vector();
	    for(int j=0;j<attrSet.size();j++){
		Object data = basetuple.dataAt(attrIndex[j]);
		present.add(data);
	    }
	    Tuple outtuple = new Tuple(present);
	    outbatch.add(outtuple);
	}
	return outbatch;
    }


    /** Close the operator */
    public boolean close(){
		return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }


    public Object clone(){
	Operator newbase = (Operator) base.clone();
	Vector newattr = new Vector();
	for(int i=0;i<attrSet.size();i++)
	    newattr.add((Attribute) ((Attribute)attrSet.elementAt(i)).clone());
	Project newproj = new Project(newbase,newattr,optype);
	Schema newSchema = newbase.getSchema().subSchema(newattr);
	newproj.setSchema(newSchema);
	return newproj;
    }
}
