package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Used for the pre-processing phase of each document in the collection
 */
public class Preprocessor {

    private HashSet<String> stopwords;

    /**
     * Stop words are loaded into a hashmap in order to make access and removal of stop words from documents faster
     * @throws IOException
     */
    public Preprocessor() throws IOException {
        stopwords = new HashSet<>();
        //stop words removal file
        File fileStopwords = new File("docs/stopwords_eng.txt");
        LineIterator it = FileUtils.lineIterator(fileStopwords, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                stopwords.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
    }

    /**
     * Preprocessing with Stop-words removal
     * In this optimised preprocessing pipelining, an attempt was made to reduce the loop for processing.
     * normalisation and tokenization was done with regex, then in one cycle are made stop_words removal and stemming.
     * @param text string in input , this is a document
     * @return terms list of word of document pre-processed
     * @throws IOException
     */
    public List<String> preprocessDocument(String text) throws IOException {
        Normalizer normalizer = new Normalizer();
        Stemmer stemmer = new Stemmer();
        //START PIPELINE
        // 1. normalization
        String outputNormalizer = normalizer.normalize(text);
        // 2. tokenization with a regex
        String[] input = outputNormalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        // iteration through all terms of document
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            //3. stopwords removal : if is not a stopwords is added
            if(!stopwords.contains(term)){
                // 4. non stopwords are stemmed
                term = stemmer.stemWord(term);
                terms.add(term);
            }
        }
        return terms;
    }

    /**
     * Preprocessing without Stop-words removal
     * Same pipeline of standard preprocessing but without step for stopwords removal.
     * @param text text string in input , this is a document
     * @return terms list of word of document pre-processed
     * @throws IOException
     */
    public List<String> preprocessDocumentUnfiltered(String text) throws IOException {
        Normalizer normalizer = new Normalizer();
        //START PIPELINE
        // 1. normalization
        String outputNormalizer = normalizer.normalize(text);
        // 2. tokenization with a regex
        String[] input = outputNormalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        // iteration through all terms of document
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            terms.add(term);
        }
        return terms;
    }

}
