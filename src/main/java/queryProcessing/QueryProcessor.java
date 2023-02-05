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

public class QueryProcessor {

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
                //if(cont>4000)System.out.println(line);
                String[] inputs = line.split("\t");
                String id = inputs[0];
                String query = inputs[1];
                //long start = System.currentTimeMillis();
                ScoreEntry entry = d.disjunctiveDaatEval(query, 1, true);
                int docno = entry.getDocID()-1;
                buffer.write(id+"\tQ0\t"+docno+"\t1\t"+entry.getScore()+"\tSTANDARD");
                buffer.newLine();
                cont++;
                if(cont%1000 == 0){
                    System.out.println(cont);
                }
                //long end = System.currentTimeMillis() - start;
                //double time = (double)end/1000.0;
                //System.out.println("Result for query " + query + " obtained in: " + time + " seconds");
                //Result obtained in: 3370.874 seconds
            }
        } finally {
            buffer.close();
            LineIterator.closeQuietly(it);
        }
        long end = System.currentTimeMillis() - start;
        double time = (double)end/1000.0;
        System.out.println("Result obtained in: " + time + " seconds");*/
        //2213.394 seconds
        /*
        [[docid=7443188, score=9.99460140426369], [docid=8474001, score=9.069019164423922], [docid=4024545, score=9.059309251704772], [docid=4326254, score=8.996953836126837], [docid=4024550, score=8.954260676830543], [docid=7039968, score=8.93573677814313], [docid=2332849, score=8.922280263405527], [docid=7659697, score=8.905726515313644], [docid=7510642, score=8.865987516251808], [docid=1001113, score=8.782835741968295]]
        Result obtained in: 0.142 seconds
         */
    }
}
