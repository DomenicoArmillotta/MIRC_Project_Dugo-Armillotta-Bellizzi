package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class PreprocessDoc {

    /**
     * In this optimised preprocessing pipelining, an attempt was made to reduce the loop for processing.
     * normalisation and tokenization was done with regex, then in one cycle are made stop_words removal and stemming.
     * @param text string in input , this is a document
     * @return terms list of word of document pre-processed
     * @throws IOException
     */
    public List<String> preprocessDocument(String text) throws IOException {
        Normalizer normalizer = new Normalizer();
        Stemmer stemmer = new Stemmer();
        //stop words removal file
        String path = "docs/stopwords_eng.txt";
        File file_stopwords = new File("docs/stopwords_eng.txt");
        Path p = Paths.get(path);
        List<String> list_stopwords = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator to improve memory efficency
        LineIterator it = FileUtils.lineIterator(file_stopwords, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_stopwords.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        //START PIPELINE
        // 1. normalization
        String output_normalizer = normalizer.normalize(text);
        // 2. tokenization with a regex
        String[] input = output_normalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        // iteration through all terms of document
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            //3. stopwords removal : if is not a stopwords is added
            if(!list_stopwords.contains(term)){
                // 4. non stopwords are stemmed
                term = stemmer.stemming_word(term);
                terms.add(term);
            }
        }
        return  terms;
    }
    public List<String> preprocessDocumentUnfiltered(String text) throws IOException {
        Normalizer normalizer = new Normalizer();
        //START PIPELINE
        // 1. normalization
        String output_normalizer = normalizer.normalize(text);
        // 2. tokenization with a regex
        String[] input = output_normalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        // iteration through all terms of document
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            terms.add(term);
        }
        return  terms;
    }


}
