/* This is main driver program of the query processor */

import java.io.*;
import java.sql.Time;

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

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        getNumBytesPerPage(in);

        String queryfile = args[0];
        String resultfile = args[1];
        SQLQuery sqlquery = getSqlQuery(queryfile);

        int numJoinOrOrderBy = sqlquery.getNumJoin() + ((sqlquery.getNumOrderBy() > 0) ? 1 : 0);
        BufferManager bm = setNumBuffers(in, numJoinOrOrderBy);
        boolean runRandomized = false;
        Operator root;
        if (runRandomized) {
            RandomOptimizer ro = new RandomOptimizer(sqlquery);
            Operator logicalroot = ro.getOptimizedPlan();

            if (logicalroot == null) {
                System.out.println("root is null");
                System.exit(1);
            }

            root = RandomOptimizer.makeExecPlan(logicalroot);
            Debug.printRed(DPoptimizer.getTreeRepresentation(root));
        } else {
            DPoptimizer dp = new DPoptimizer(sqlquery);
            root = RandomOptimizer.makeExecPlan(dp.getBestPlan());
        }

        Debug.printWithLines(true,"Execution Plan");
        Debug.PPrint(root);
        System.out.println();

        confirmExec(in);

        System.out.println("Starting operation now...");
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

        writeResultToFile(root);

        out.close();

        long endtime = System.currentTimeMillis();
        double executiontime = (endtime - starttime) / 1000.0;
        System.out.println("Execution time = " + executiontime);
    }

    private static void writeResultToFile(Operator root) {
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
    }

    private static BufferManager setNumBuffers(BufferedReader in, int numJoin) {
        /* If there are joins, then assign buffers to each join operator while preparing the plan */
        /* As buffer manager is not implemented, just input the number of buffers available */
        BufferManager bm = null;
        if (numJoin != 0) {
            System.out.println("enter the number of buffers available");
            try {
                String temp = in.readLine();
                int numBuff = Integer.parseInt(temp);
                bm = new BufferManager(numBuff, numJoin);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /* Check the number of buffers available is enough */
        int numBuff = BufferManager.getBuffersPerJoinOrOrderBy();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per join operator ");
            System.exit(1);
        }
        return bm;
    }

    public static SQLQuery getSqlQuery(String queryfile) {
        FileInputStream source = ReadQueryFile(queryfile);

        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);

        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occurred while parsing");
            System.exit(1);
        }

        /* SQLQuery is the result of the parsing */
        return p.getSQLQuery();
    }

    private static FileInputStream ReadQueryFile(String queryfile) {
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }
        return source;
    }

    private static void getNumBytesPerPage(BufferedReader in) {
        System.out.println("enter the number of bytes per page");
        try {
            String temp = in.readLine();
            int pagesize = Integer.parseInt(temp);
            Batch.setPageSize(pagesize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void confirmExec(BufferedReader in) {
        System.out.println("enter 1 to continue, 0 to abort ");
        try {
            String temp = in.readLine();
            int flag = Integer.parseInt(temp);
            if (flag == 0) {
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
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
                out.print(((Float) data).floatValue() + "\t");}
               else if (data instanceof Long) {
                    out.print(new Time((long) data).toString() + "\t");
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