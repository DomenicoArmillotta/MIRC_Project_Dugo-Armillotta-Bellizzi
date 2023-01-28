package queryProcessing;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.IOException;
import java.util.Scanner;

public class QueryProcessor {

    public static void main(String[] args) throws IOException {
        Daat d = new Daat();
        while(true) {
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
            long time = end*1000;
            System.out.println("Result obtained in: " + time + " seconds");
        }
    }
}
