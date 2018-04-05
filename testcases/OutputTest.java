import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import qp.operators.Debug;
import qp.operators.JoinType;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.DPoptimizer;
import qp.optimizer.RandomOptimizer;
import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OutputTest {

    @Rule
    public TemporaryFolder saveFolder = new TemporaryFolder();
    private String folderRoot;
    private String[] queryFiles = {"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"};
    private String[] resultFiles = { "out1", "out2", "out3", "out4", "out5", "out6", "out7", "out8", "out9", "out10" };
    private ArrayList<SQLQuery> sqlQueries = new ArrayList<>();
    private String outputFileToTest;
    private PrintWriter out;
    private int numAtts;
    private enum testType {
        Random, DP
    }

    private testType toTest = testType.DP;
    private static boolean DEBUG = true;

    private String schema1;
    private String schema2;

    @Before
    public void setUp() {
        for (String file: queryFiles) {
            sqlQueries.add(QueryMain.getSqlQuery(file));
        }

        Batch.setPageSize(1000); // bytes per page
        folderRoot = saveFolder.getRoot().getPath();
        outputFileToTest = folderRoot + "out-test";
    }

    @Test
    public void testPageNestedJoin() throws Exception {
        JoinType.setNumJoinTypes(1);
        compareJoinResults();
    }

    @Test
    public void testBlockNestedJoin() throws Exception {
        JoinType.setNumJoinTypes(2);
        compareJoinResults();
    }

    @Test
    public void testSortMergeJoin() throws Exception {
        // the error is the when the join is flipped, it produces different number of tuples after the join
        JoinType.setNumJoinTypes(3);
        compareJoinResults();
    }

    public void compareJoinResults() throws Exception {
        for (int i = 0; i < sqlQueries.size(); i++) {
            Debug.printBold((i+1) + "th iteration");
            SQLQuery query = sqlQueries.get(i);
            if (query.getNumJoin() > 0) {
                BufferManager bf = new BufferManager(10, query.getNumJoin());
            }
            String resultFile = resultFiles[i];

            BufferManager.numJoin = query.getNumJoin();
            // run the join we want to test
            saveToTempFile(query, outputFileToTest);

            // sort the two files to compare
            String fileStored1 = sortFile(outputFileToTest);
            String fileStored2 = sortFile(resultFile);
            assertSchemasSame(schema1, schema2);
            assertTrue("The query output matches for q" + (i+1), assertFileSame(fileStored1, fileStored2));
        }

    }

    private void saveToTempFile(SQLQuery query, String tempFileName) throws IOException {
        Operator root;
        if (toTest == testType.Random)
            root = runRandomOptimizer(query);
        else
            root = runDPOptimizer(query);

        Debug.PPrint(root);
        System.out.println();
        assertTrue("root opens", root.open());
        out = new PrintWriter(new BufferedWriter(new FileWriter(tempFileName)));
        writeResultToFile(root);
        out.close();
    }

    private Operator runDPOptimizer(SQLQuery query) {
        DPoptimizer dp = new DPoptimizer(query);
        return DPoptimizer.makeExecPlan(dp.getBestPlan());
    }

    private Operator runRandomOptimizer(SQLQuery query) {
        RandomOptimizer ro = new RandomOptimizer(query);
        Operator logicalroot = ro.getOptimizedPlan();
        assertTrue("logical root is not null", logicalroot != null);

        /* preparing the execution plan */
        return RandomOptimizer.makeExecPlan(logicalroot);
    }

    public void writeResultToFile(Operator root) {
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);
        Batch resultbatch;
        int tupleCount = 0;

        /* print each tuple in the result */
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); i++) {
                printTuple(resultbatch.elementAt(i));
                tupleCount++;
            }
        }
        Debug.printBold("#tuples = " + tupleCount);
        root.close();
    }

    protected void printSchema(Schema schema) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numAtts; i++) {
            Attribute attr = schema.getAttribute(i);
//            out.print(attr.getTabName() + "." + attr.getColName() + "  ");
            sb.append(attr.getTabName());
            sb.append(".");
            sb.append(attr.getColName());
            sb.append("  ");
        }
        sb.append("\n");
        schema1 = sb.toString();
        out.print(schema1);
    }

    /**
     * outputs a tuple in the result query into file
     * @param t tuple
     */
    protected void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; i++) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(data + "\t");
            } else if (data instanceof Float) {
                out.print(data + "\t");
            } else {
                out.print(data + "\t");
            }
        }
        out.println();
    }

    /**
     * Sorts the file according to some key. This is required because depending on the join methods,
     * the output of the join will differ in the ordering of tuples
     * @param filename name of the file to sort
     * @return the name of the sorted file
     */
    private String sortFile(String filename) throws Exception {
        String fileStored = filename + "_sorted";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        if (!filename.contains(folderRoot)) {
            // this is for the result file
            fileStored = folderRoot + fileStored;
            schema2 = reader.readLine();
        } else {
            schema1 = reader.readLine();
        }

        Map<String, String> map=new TreeMap<>();

        String line="";
        while((line=reader.readLine())!=null){
            map.put(getField(line),line);
        }
        reader.close();
        FileWriter writer = new FileWriter(fileStored);
        for(String val : map.values()){
            writer.write(val);
            writer.write('\n');
        }
        writer.close();
        return fileStored;
    }

    private static String getField(String line) {
        return line.split(" ")[0];//extract value you want to sort on. now just use the first token
    }

    private static boolean assertFileSame (String file1, String file2) throws Exception {
        BufferedReader reader1 = new BufferedReader(new FileReader(file1));
        BufferedReader reader2 = new BufferedReader(new FileReader(file2));

        String line1; // the one we are testing
        while ( (line1 = reader1.readLine()) != null) {
            String line2 = reader2.readLine();
            if (DEBUG) {
                Debug.printRed("line1 " + line1);
                Debug.printBold("line2 " + line2);
            }
//            if (!line1.equals(line2))
//                return false;
        }
        String line2 = reader2.readLine();
        if (line2 != null) {
            return false;
        }


        reader1.close();
        reader2.close();
        return true;
    }

    private static void assertSchemasSame(String line1, String line2) throws Exception {
        System.out.println("Schema of 1: " + line1);
        System.out.println("Schema of 2: " + line2);
        assertEquals("Schemas match", line1, line2);
    }
}