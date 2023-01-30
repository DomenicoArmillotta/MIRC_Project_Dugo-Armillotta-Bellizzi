package indexing;

import java.io.IOException;

public class Indexer {

    public static void main(String[] args) throws IOException {
        SPIMI s = new SPIMI();
        long start = System.currentTimeMillis();
        s.spimiInvertBlock("docs/collection.tsv", true);
        long end = System.currentTimeMillis() - start;
        long time = end/1000;
        System.out.println("Result obtained in: " + time + " seconds");
    }
}
