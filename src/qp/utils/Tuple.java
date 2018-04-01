/**********
 Tuple container class
 **********/
package qp.utils;
import java.util.LinkedList;
import java.util.Vector;
import java.io.Serializable;

/** Tuple - a simple object which holds a Vector
     of data */

public class Tuple implements Serializable {
    public Vector _data;

    public Tuple(Vector d){
    	_data=d;
    }

    /** Accessor for data */
    public Vector data() {
        return _data;
    }

    public Object dataAt(int index){
    	return _data.elementAt(index);
    }

    /** Checks whether the join condition is satisfied or not
     ** before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, int leftindex, int rightindex){
		Object leftData = dataAt(leftindex);
		Object rightData = right.dataAt(rightindex);
	
		if(leftData.equals(rightData))
		    return true;
		else
		    return false;
	}

    /** Joining two tuples Without duplicate column elimination**/
    public Tuple joinWith(Tuple right){ //, Attribute leftAttr, Attribute rightAttr){
		Vector newData = new Vector(this.data());
		newData.addAll(right.data());
		return new Tuple(newData);
    }

	/** Compare two tuples in the same table on given attribute **/
    public static int compareTuples(Tuple left,Tuple right, int index){
    	return compareTuples(left,right,index,index);
    }
    
    /** Compare two tuples in the same table on a given list of attributes **/
    public static int compareTuplesWith(Tuple left, Tuple right, int[] attrIndices) {
    	for (int attrIndex: attrIndices) { 
			int compareAtIndex = compareTuples(left, right, attrIndex);
			if (compareAtIndex != 0) {
				return compareAtIndex;
			}
		}
		return 0;
    }

    /** comparing tuples in different tables, used for join condition checking **/
    public static int compareTuples( Tuple left,Tuple right, int leftIndex, int rightIndex){
		Object leftdata = left.dataAt(leftIndex);
		Object rightdata = right.dataAt(rightIndex);
		if(leftdata instanceof Integer){
		    return ((Integer)leftdata).compareTo((Integer)rightdata);
		} else if(leftdata instanceof String){
		    return ((String)leftdata).compareTo((String)rightdata);
	
		} else if(leftdata instanceof Float){
		    return ((Float)leftdata).compareTo((Float)rightdata);
		} else{
		    System.out.println("Tuple: Unknown comparision of the tuples");
		    System.exit(1);
		    return 0;
		}
    }
}










