import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import qp.operators.Debug;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.DPoptimizer;
import qp.optimizer.RandomOptimizer;
import qp.utils.Batch;
import qp.utils.SQLQuery;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class OutputTest {

    @Rule
    public TemporaryFolder saveFolder = new TemporaryFolder();
    private String folderRoot;
    private String[] queryFiles = {"query3"};
    private String[] resultFiles = { "result" };
    private ArrayList<SQLQuery> sqlQueries = new ArrayList<>();
    private String outputFileToTest;
    private PrintWriter out;
    private int numAtts;
    private boolean testRandom = false;

    @Before
    public void setUp() {
        for (String file: queryFiles) {
            sqlQueries.add(QueryMain.getSqlQuery(file));
        }

        Batch.setPageSize(10000); // bytes per page
        BufferManager.numBuffer = 1000;
        folderRoot = saveFolder.getRoot().getPath();
        outputFileToTest = folderRoot + "out-test";
    }

    @Test
    public void compareJoinResults() throws Exception {
        Operator root;
        boolean isOutputCorrect = true;
        for (int i = 0; i < sqlQueries.size(); i++) {
            SQLQuery query = sqlQueries.get(i);
            String resultFile = resultFiles[i];

            BufferManager.numJoin = query.getNumJoin();
            // run the join we want to test
            saveToTempFile(query, outputFileToTest);

            // sort the two files to compare
            String fileStored2 = sortFile(outputFileToTest);
            String fileStored1 = sortFile(resultFile);

            assertTrue("The two output files after sort should be the same", assertFileSame(fileStored1, fileStored2));
//            if (!assertFileSame(fileStored1, fileStored2)) {
//                isOutputCorrect = false;
//            }
        }

//        assertTrue("The produced query results are the same", isOutputCorrect);
    }

    private void saveToTempFile(SQLQuery query, String tempFileName) throws IOException {
        Operator root;
        if (testRandom)
            root = runRandomOptimizer(query);
        else
            root = runDPOptimizer(query);

        assertTrue("root opens", root.open());
        out = new PrintWriter(new BufferedWriter(new FileWriter(tempFileName)));
        writeResultToFile(root);
        out.close();
    }

    private Operator runDPOptimizer(SQLQuery query) {
        DPoptimizer dp = new DPoptimizer(query);
        return RandomOptimizer.makeExecPlan(dp.getBestPlan());
    }

    private Operator runRandomOptimizer(SQLQuery query) {
        RandomOptimizer ro = new RandomOptimizer(query);
        Operator logicalroot = ro.getOptimizedPlan();
//        assertTrue("logical root is not null", logicalroot != null);

        /* preparing the execution plan */
        return RandomOptimizer.makeExecPlan(logicalroot);
    }

    public void writeResultToFile(Operator root) {
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        Batch resultbatch;

        /* print each tuple in the result */
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); i++) {
                printTuple(resultbatch.elementAt(i));
            }
        }
        root.close();
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
    private static String sortFile(String filename) throws Exception {
        String fileStored = filename + "_sorted";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        Map<String, String> map=new TreeMap<String, String>();
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

        String line1;
        while ( (line1 = reader1.readLine()) != null) {
            if (!line1.equals(reader2.readLine()))
                return false;
        }

        if (reader2.readLine() != null)
            return false;

        reader1.close();
        reader2.close();
        return true;
    }

}