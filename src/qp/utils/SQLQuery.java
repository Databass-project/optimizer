/* the format of the parse SQL query, also see readme file */
package qp.utils;

import java.util.Vector;

public class SQLQuery {

    Vector projectList;     //List of project attributes from select clause
    Vector fromList;        // List of tables in from clause
    Vector conditionList;   // List of conditions appeared in where clause

    /*represent again the the selection and join conditions in separate lists */
    Vector selectionList;    //List of select predicates
    Vector joinList;           //List of join predicates

    Vector groupbyList;        //List of attributes in GROUP BY clause
    boolean isDistinct = false;   // Whether distinct key word appeared in select clause

    public SQLQuery(Vector list1, Vector list2, Vector list3, Vector list4) {
        projectList = list1;
        fromList = list2;
        conditionList = list3;
        groupbyList = list4;
        splitConditionList(conditionList);
    }

    public SQLQuery(Vector list1, Vector list2, Vector list3) {
        projectList = list1;
        fromList = list2;
        conditionList = list3;
        groupbyList = null;
        splitConditionList(conditionList);
    }

    // 12 july 2003 (whtok)
    // Constructor to handle no WHERE clause case
    public SQLQuery(Vector list1, Vector list2) {
        projectList = list1;
        fromList = list2;
        conditionList = null;
        groupbyList = null;
        joinList = new Vector();
        selectionList = new Vector();
    }

    /**
     * split the condition list into selection, and join list
     **/
    protected void splitConditionList(Vector tempVector) {
        selectionList = new Vector();
        joinList = new Vector();
        for (int i = 0; i < tempVector.size(); i++) {
            Condition cn = (Condition) tempVector.elementAt(i);
            if (cn.getOpType() == Condition.SELECT)
                selectionList.add(cn);
            else
                joinList.add(cn);
        }
    }

    public void setIsDistinct(boolean flag) {
        isDistinct = flag;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public Vector getProjectList() {
        return projectList;
    }

    public Vector getFromList() {
        return fromList;
    }

    public Vector getConditionList() {
        return conditionList;
    }

    public Vector getSelectionList() {
        return selectionList;
    }

    public Vector getJoinList() {
        return joinList;
    }

    public void setGroupByList(Vector list) {
        groupbyList = list;
    }

    public Vector getGroupByList() {
        return groupbyList;
    }

    public int getNumJoin() {
        if (joinList == null)
            return 0;

        return joinList.size();
    }

}
