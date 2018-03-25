/* This is main driver program of the query processor */

import java.io.*;

import qp.utils.*;
import qp.operators.*;
import qp.optimizer.*;
import qp.parser.*;

public class QueryMain {

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile>");
            System.exit(1);
        }

        /* Enter the number of bytes per page */
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("enter the number of bytes per page");
        String temp;
        try {
            temp = in.readLine();
            int pagesize = Integer.parseInt(temp);
            Batch.setPageSize(pagesize);
        } catch (Exception e) {
            e.printStackTrace();
        }


        String queryfile = args[0];
        String resultfile = args[1];
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }


        /* scan the query */
        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);


        /* parse the query */
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occurred while parsing");
            System.exit(1);
        }

        /* SQLQuery is the result of the parsing */

        SQLQuery sqlquery = p.getSQLQuery();
        int numJoin = sqlquery.getNumJoin();


        /* If there are joins, then assign buffers to each join operator while preparing the plan */
        /* As buffer manager is not implemented, just input the number of buffers available */
        if (numJoin != 0) {
            System.out.println("enter the number of buffers available");
            try {
                temp = in.readLine();
                int numBuff = Integer.parseInt(temp);
                BufferManager bm = new BufferManager(numBuff, numJoin);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        /* Check the number of buffers available is enough */
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per a join operator ");
            System.exit(1);
        }


        /* This part is used When some random initial plan is required instead of comple optimized plan **/
        /*

         RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
         Operator logicalroot = rip.prepareInitialPlan();
         PlanCost pc = new PlanCost();
         int initCost = pc.getCost(logicalroot);
         Debug.PPrint(logicalroot);
         System.out.print("   "+initCost);
         System.out.println();
         */


        /* Use random Optimization algorithm to get a random optimized execution plan */

        RandomOptimizer ro = new RandomOptimizer(sqlquery);
        Operator logicalroot = ro.getOptimizedPlan();
        if (logicalroot == null) {
            System.out.println("root is null");
            System.exit(1);
        }


        /* preparing the execution plan */
        Operator root = RandomOptimizer.makeExecPlan(logicalroot);

        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        System.out.println();

        /* Ask user whether to continue execution of the program */
        System.out.println("enter 1 to continue, 0 to abort ");

        try {
            temp = in.readLine();
            int flag = Integer.parseInt(temp);
            if (flag == 0) {
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        long starttime = System.currentTimeMillis();

        if (!root.open()) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultfile);
            System.exit(1);
        }


        /* print the schema of the result */
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);
        Batch resultbatch;


        /* print each tuple in the result */
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); i++) {
                printTuple(resultbatch.elementAt(i));
            }
        }
        root.close();
        out.close();

        long endtime = System.currentTimeMillis();
        double executiontime = (endtime - starttime) / 1000.0;
        System.out.println("Execution time = " + executiontime);

    }

    /**
     * outputs a tuple in the result query into file
     * @param t tuple
     */
    protected static void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; i++) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }

    /**
     * outputs the table name and column names in the result query into file
     * @param schema the table schema
     */
    protected static void printSchema(Schema schema) {
        for (int i = 0; i < numAtts; i++) {
            Attribute attr = schema.getAttribute(i);
            out.print(attr.getTabName() + "." + attr.getColName() + "  ");
        }
        out.println();
    }

}







