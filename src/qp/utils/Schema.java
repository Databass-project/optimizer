/** schema of the table/result, and is attached to every operator**/


package qp.utils;

import java.io.Serializable;
import java.util.Vector;

public class Schema implements Serializable{

    Vector attset;         // the attributes belong to this schema
    int tuplesize; // Number of bytes required for this tuple (size of record)

    public Schema(Vector colset){

	attset = colset;

    }

	public void setTupleSize(int size){
		tuplesize=size;
	}

	public int getTupleSize(){
		return tuplesize;
	}

    public int getNumCols(){
	return attset.size();
    }


    public void add(Attribute attr){
	attset.add(attr);
    }

    public Vector getAttList(){
	return attset;
    }


    public Attribute getAttribute(int i){
	return (Attribute) attset.elementAt(i);
    }

    public int indexOf(Attribute tarattr){
	for(int i=0;i<attset.size();i++){
	    Attribute attr = (Attribute) attset.elementAt(i);
	    if(attr.equals(tarattr)){
		return i;
	    }
	}
	return -1;
    }

    public int typeOf(Attribute tarattr){
	for(int i=0;i<attset.size();i++){
	    Attribute attr = (Attribute) attset.elementAt(i);
	    if(attr.equals(tarattr)){
		return attr.getType();
	    }
	}
	return -1;
    }

    public int typeOf(int attrAt){
	Attribute attr =(Attribute) attset.elementAt(attrAt);
	return attr.getType();
    }



    /** Checks whether given attribute is present in this
	Schema or not
    **/

    public boolean contains(Attribute tarattr){
	for(int i=0;i<attset.size();i++){
	    Attribute attr = (Attribute)attset.elementAt(i);
	    if(attr.equals(tarattr)){
		return true;
	    }
	}
	return false;
    }



    /**The schema of resultant join operation, Not considered the elimination of duplicate Column **/

    public Schema joinWith(Schema right){ //, Attribute leftAttr, Attribute rightAttr){
	Vector newSchema = new Vector(this.attset);
	newSchema.addAll(right.getAttList());

	int newtupsize= this.getTupleSize()+right.getTupleSize();
	Schema newSche = new Schema(newSchema);
	newSche.setTupleSize(newtupsize);
	return newSche;
	//return new Schema(newSchema);
    }


	/** To get schema due to result of project operation
		attrlist is the attirbuted that are projected
		**/

    public Schema subSchema(Vector attrlist){
	Vector newVec = new Vector();
	int newtupsize=0;
	for(int i=0;i<attrlist.size();i++){
	    Attribute resAttr = (Attribute) attrlist.elementAt(i);
	    int baseIndex = this.indexOf(resAttr);
	    Attribute baseAttr = this.getAttribute(baseIndex);
	    newVec.add(baseAttr);
		newtupsize=newtupsize+baseAttr.getAttrSize();
	}
	Schema newsche = new Schema(newVec);
	newsche.setTupleSize(newtupsize);
	return newsche;
    }



  public Object clone(){
	Vector newvec = new Vector();
	for(int i=0;i<attset.size();i++){
	    Attribute newatt = (Attribute) ((Attribute)attset.elementAt(i)).clone();
	    newvec.add(newatt);
	}
	Schema newsche = new Schema(newvec);
	newsche.setTupleSize(tuplesize);

	return newsche;
	//return new Schema(newvec);
    }

}





