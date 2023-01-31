package queryProcessing;


import java.io.IOException;
import java.util.Scanner;

public class QueryProcessor {

    public static void main(String[] args) throws IOException {
        System.out.println("LOADING DATA STRUCTURES");
        Daat d = new Daat();
        System.out.println("PROGRAM STARTED");
        long start = System.currentTimeMillis();
        int k = 10;
        //String query = "who is the president of the usa";
        String query = "what is stomach bile acid";
        /*Results should be:
        [[docid=7443188, score=9.99322227476917], [docid=8474001, score=9.066945143488162], [docid=4024545, score=9.056948903998137], [docid=4326254, score=8.994563026789095], [docid=4024550, score=8.951834637529897], [docid=7039968, score=8.933425154704048], [docid=2332849, score=8.920170660835113], [docid=7659697, score=8.903379989877958], [docid=7510642, score=8.863646998525159], [docid=1001113, score=8.780602504235764]]
         */
        System.out.println(query);
        System.out.println(d.disjunctiveDaat(query, k, true));
        long end = System.currentTimeMillis() - start;
        double time = (double)end/1000.0;
        System.out.println("Result obtained in: " + time + " seconds");
        /*while(true) {
            System.out.print("ENTER A QUERY OR 'END' TO END THE PROGRAM");
            System.out.print("mode: 0 for conjunctive, 1 for disjunctive");
            System.out.print("scoring_function: 0 for tfidf, 1 for bm25");
            System.out.print("Enter a query in the format 'query rank mode scoring_function'");
            Scanner sc = new Scanner(System.in); //System.in is a standard input stream
            String line = sc.nextLine();
            if(line.equals("END")) break;
            String input[] = line.split(" ");
            String query = input[0];
            int k = Integer.parseInt(input[1]);
            Boolean mode = Boolean.parseBoolean(input[2]);
            Boolean score = Boolean.parseBoolean(input[3]);
            long start = System.currentTimeMillis();
            if (mode) {
                System.out.println(d.disjunctiveDaat(query, k));
            } else {
                System.out.println(d.conjunctiveDaat(query, k));
            }
            long end = System.currentTimeMillis() - start;
            long time = end/1000;
            System.out.println("Result obtained in: " + time + " seconds");
        }*/
    }
}
