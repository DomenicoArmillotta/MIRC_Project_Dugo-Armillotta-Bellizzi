package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Preprocess_doc {
    /**
     * given a string, runs the preprocessing pipeline and returns a processed word list.
     * In order it performs: tokenization , stopword removal , stemming
     * @param s string of document to elaborate
     * @return output_stopwords_removal
     * @throws IOException
     */
    public List<String> preprocess_doc (String s) throws IOException {
        Normalizer normalizer = new Normalizer();
        Tokenizer tokenizer = new Tokenizer();
        Stopword_removal stopword_removal = new Stopword_removal();
        Stemmer stemmer = new Stemmer();
        String output_normalizer = normalizer.normalize(s);
        List<String> output_tokenizer = new ArrayList<>();
        output_tokenizer = tokenizer.tokenize(output_normalizer);
        List<String> output_stopwords_removal = new ArrayList<>();
        output_stopwords_removal = stopword_removal.remove(output_tokenizer);
        List<String> output_stemming = new ArrayList<>();
        output_stemming = stemmer.stemming(output_stopwords_removal);
        return output_stemming;
    }

    public List<String> preprocess_doc_optimized (String s) throws IOException {
        //faccio il normalizer
        //poi messo insieme tokenizer , stopword removal e stemmer
        Normalizer normalizer = new Normalizer();
        Stemmer stemmer = new Stemmer();
        //stop words removal
        String path = "docs/stopwords_eng.txt";
        File file_stopwords = new File("docs/stopwords_eng.txt");
        Path p = Paths.get(path);
        List<String> list_stopwords = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator
        LineIterator it = FileUtils.lineIterator(file_stopwords, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_stopwords.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        //START
        String output_normalizer = normalizer.normalize(s);
        String[] input = output_normalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        //sbagliato da rivedere --> le stop words le devo iterare tutte per forza quindi o(n) sulle stopwords
        boolean stop_finded = false;
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            if(!list_stopwords.contains(term)){
                term = stemmer.stemming_word(term);
                terms.add(term);
            }
        }
        return  terms;
    }




}
