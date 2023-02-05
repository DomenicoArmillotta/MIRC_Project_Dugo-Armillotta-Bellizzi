package indexing;

import java.io.IOException;

/**
 * Read the collection to index , and execute the pipeline to create data structures
 * in output is given the time elapsed during this phase
 */
public class Indexer {

    /**
     * main for the indexing phase.
     * @param args not used
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        SPIMI s = new SPIMI();
        long start = System.currentTimeMillis();
        s.spimiInvertBlock("docs/collection.zip", true); //false: unfiltered, true: filtered
        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("Result obtained in: " + time + " minutes");
    }
}
