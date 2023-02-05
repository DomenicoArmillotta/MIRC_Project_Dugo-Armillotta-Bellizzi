package queryProcessing;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Second phase of the engine , query processing menu
 */
public class QueryProcessor {

    /**
     * main to start the query processing of the engine
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println("LOADING DATA STRUCTURES");
        Daat d = new Daat();
        System.out.println("PROGRAM STARTED");
        while(true) {
            Scanner sc = new Scanner(System.in); //System.in is a standard input stream
            System.out.println("Enter a query or END to end the program");
            String line = sc.nextLine();
            if(line.equals("END")) break;
            String query = line;
            System.out.println("mode: 0 for conjunctive, 1 for disjunctive");
            line = sc.nextLine();
            int mode = Integer.parseInt(line);
            if(mode>1)continue;
            System.out.println("scoring function: 0 for tfidf, 1 for bm25");
            line = sc.nextLine();
            int score = Integer.parseInt(line);
            if(score>1)continue;
            System.out.println("Insert rank (greater than 0)");
            line = sc.nextLine();
            int k = Integer.parseInt(line);
            if(k<1) continue;
            System.out.println(query + " " + mode + " " + score + " " + k);
            long start = System.currentTimeMillis();
            if (mode == 1) {
                System.out.println(d.disjunctiveDaat(query, k, score));
            } else {
                System.out.println(d.conjunctiveDaat(query, k, score));
            }
            long end = System.currentTimeMillis() - start;
            double time = end/1000.0;
            System.out.println("Result obtained in: " + time + " seconds");
        }

        //USED TO TEST TREC EVAL
        /*long start = System.currentTimeMillis();
        File queryFile = new File("docs/queries.eval.tsv");
        String resultsPath = "docs/results_file.tsv";
        FileWriter file = new FileWriter(resultsPath);
        BufferedWriter buffer = new BufferedWriter(file);
        LineIterator it = FileUtils.lineIterator(queryFile, "UTF-8");


        try {
            int cont = 0;
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] inputs = line.split("\t");
                String id = inputs[0];
                String query = inputs[1];
                //long start = System.currentTimeMillis();
                ScoreEntry entry = d.disjunctiveDaatEval(query, 1, true);
                int docno = entry.getDocID()-1;
                buffer.write(id+"\tQ0\t"+docno+"\t1\t"+entry.getScore()+"\tSTANDARD");
                buffer.newLine();
                cont++;
            }
        } finally {
            buffer.close();
            LineIterator.closeQuietly(it);
        }
        long end = System.currentTimeMillis() - start;
        double time = (double)end/1000.0;
        System.out.println("Result obtained in: " + time + " seconds");
         */
    }
}
